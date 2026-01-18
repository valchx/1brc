const std = @import("std");

const Record = struct {
    const Self = @This();

    min: f32 = std.math.floatMax(f32),
    max: f32 = std.math.floatMax(f32),
    mean: f32 = 0,
    count: u32 = 0,

    pub fn init(value: f32) Self {
        return .{
            .min = value,
            .max = value,
            .mean = value,
            .count = 1,
        };
    }

    pub fn add(self: *Self, value: f32) void {
        self.max = @max(self.max, value);
        self.min = @min(self.min, value);
        self.addToMean(value, 1);
    }

    pub fn addToMean(self: *Self, value: f32, count: u32) void {
        const f_old_count: f32 = @floatFromInt(self.count);
        const f_count: f32 = @floatFromInt(count);

        self.count += count;
        self.mean += (value - self.mean) * (f_count / (f_old_count + f_count));
    }
};

const Records = struct {
    const Self = @This();

    map: std.StringHashMap(Record),
    mutex: std.Thread.Mutex = .{},

    pub fn init(alloc: std.mem.Allocator) Self {
        const map = std.StringHashMap(Record).init(alloc);

        return Self{
            .map = map,
        };
    }

    pub fn deinit(self: *Self) void {
        // TODO : Free all the keys ? This is allocator agnostic
        self.map.deinit();
    }

    pub fn update(self: *Self, key: []const u8, value: f32) !void {
        if (self.map.getPtr(key)) |entry| {
            entry.add(value);
        } else {
            // TODO : Use map's allocator from the struct directly.
            const own_key = try self.map.allocator.alloc(u8, key.len);
            @memcpy(own_key, key);

            try self.map.put(own_key, .init(value));
        }
    }

    pub fn print(self: Self, io: std.Io) !void {
        const stdout_file = std.Io.File.stdout();
        var buffer: [BUF_SIZE]u8 = undefined;
        var stdout = stdout_file.writer(io, &buffer);

        try stdout.interface.writeByte('{');

        var first_line_printed = false;

        var entries = self.map.iterator();
        while (entries.next()) |entry| {
            if (first_line_printed) {
                _ = try stdout.interface.write(", ");
            }

            const city_name = entry.key_ptr.*;
            const city_data = entry.value_ptr.*;

            var line_buf: [512]u8 = undefined;

            _ = try stdout.interface.write(
                try std.fmt.bufPrint(&line_buf, "{s}={}/{}/{}", .{
                    city_name,
                    city_data.min,
                    city_data.mean,
                    city_data.max,
                }),
            );

            first_line_printed = true;
        } else {}

        try stdout.interface.writeByte('}');

        try stdout.interface.flush();
    }

    pub fn merge(self: *Self, records: Self) !void {
        self.mutex.lock();
        defer self.mutex.unlock();

        var incoming_entries = records.map.iterator();
        while (incoming_entries.next()) |entry| {
            const city_name = entry.key_ptr.*;
            const city_data = entry.value_ptr.*;

            // std.debug.print("Merging {s}\n", .{city_name});
            if (self.map.getPtr(city_name)) |own_record| {
                // std.debug.print("\tMin {} + {}\n", .{ own_record.min, city_data.min });
                // std.debug.print("\tMax {} + {}\n", .{ own_record.max, city_data.max });
                // std.debug.print("\tMean {} + {}\n", .{ own_record.mean, city_data.mean });
                // std.debug.print("\tCount {} + {}\n", .{ own_record.count, city_data.count });

                own_record.min = @min(own_record.min, city_data.min);
                own_record.max = @max(own_record.max, city_data.max);
                own_record.addToMean(city_data.mean, city_data.count);
            } else {
                const own_key = try self.map.allocator.alloc(u8, city_name.len);
                @memcpy(own_key, city_name);
                // std.debug.print("\tMin {}\n", .{city_data.min});
                // std.debug.print("\tMax {}\n", .{city_data.max});
                // std.debug.print("\tMean {}\n", .{city_data.mean});
                // std.debug.print("\tCount {}\n", .{city_data.count});

                try self.map.put(own_key, city_data);
            }
        } else {}
    }
};

// 1 MiB
const BUF_SIZE = 1024 * 1024;

fn readFileChunk(
    io: std.Io,
    file_path: []const u8,
    approx_start: u64,
    approx_end: u64,
    master_records: *Records,
) !void {
    var records_arena = std.heap.ArenaAllocator.init(std.heap.page_allocator);
    defer records_arena.deinit();

    var records = Records.init(records_arena.allocator());
    defer records.deinit();

    const file = std.Io.Dir.cwd().openFile(
        io,
        file_path,
        .{
            .mode = .read_only,
        },
    ) catch |err| {
        std.debug.print("ERR: {any}", .{err});
        @panic("Could not open file.");
    };
    defer file.close(io);

    var buffer: [BUF_SIZE]u8 = undefined;
    var reader = file.reader(io, &buffer);

    // How to get the start & end ?
    // Start = if chunk != 0 : chunk * chunk size + find '\n' + 1
    //         else start is 0
    try reader.seekTo(approx_start);
    if (approx_start != 0) {
        // We skip any potentionnaly partial lines
        _ = try reader.interface.takeDelimiterExclusive('\n');
        reader.interface.toss(1);
    }

    // TODO : `takeDelimiterExclusive` should fills the available buffer.
    while (reader.interface.takeDelimiterExclusive('\n')) |line| {
        reader.interface.toss(1);

        var line_iter = std.mem.splitAny(u8, line, ";");
        const city_name = line_iter.next() orelse {
            return error.ParseError;
        };

        const city_temp = brk: {
            const str = line_iter.next() orelse {
                return error.ParseError;
            };

            break :brk try std.fmt.parseFloat(f32, str);
        };

        try records.update(city_name, city_temp);

        // End = start + chunk size + find next '\n'
        if (reader.logicalPos() > approx_end) {
            break;
        }
    } else |err| {
        switch (err) {
            error.EndOfStream => {},
            else => return error.ReaderError,
        }
    }

    // TODO : Try to merge on the master thread to see the impact of thread safety on performance.
    try master_records.merge(records);
}

fn readFileChunkUnsafe(
    io: std.Io,
    file_path: []const u8,
    approx_start: u64,
    approx_end: u64,
    master_records: *Records,
) void {
    return readFileChunk(
        io,
        file_path,
        approx_start,
        approx_end,
        master_records,
    ) catch |e| {
        std.debug.print("ERR: {any}", .{e});
        @panic("readFileChunk failed");
    };
}

pub fn main(init: std.process.Init) !void {
    var args = init.minimal.args.iterate();

    _ = args.next();

    const file_path = args.next() orelse @panic("File path not provided.");

    var file = try std.Io.Dir.cwd().openFile(init.io, file_path, .{ .mode = .read_only });
    const file_stat = try file.stat(init.io);
    const file_size = file_stat.size;
    file.close(init.io);

    var records_arena = std.heap.ArenaAllocator.init(std.heap.page_allocator);
    defer records_arena.deinit();
    // TODO : Remove this once to see race condition break.
    var thread_safe = std.heap.ThreadSafeAllocator{
        .child_allocator = records_arena.allocator(),
    };
    var records = Records.init(thread_safe.allocator());

    const thread_count = std.Thread.getCpuCount() catch {
        return error.NoThreadLimit;
    };
    if (file_size > 128 * thread_count) {
        var threaded = std.Io.Threaded.init(init.gpa, .{ .environ = init.minimal.environ });
        defer threaded.deinit();
        const io = threaded.io();
        var group = std.Io.Group.init;

        for (0..thread_count) |i| {
            const chunk_size = @divFloor(file_size, @as(u64, @intCast(thread_count)));
            const start_byte = chunk_size * i;
            const end_byte = start_byte + chunk_size;

            try group.concurrent(io, readFileChunkUnsafe, .{
                io,
                file_path,
                start_byte,
                end_byte,
                &records,
            });
        }

        try group.await(io);
    } else {
        var threaded = std.Io.Threaded.init_single_threaded;
        const io = threaded.io();
        try readFileChunk(
            io,
            file_path,
            0,
            file_size,
            &records,
        );
    }

    try records.print(init.io);
}
