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
        self.count += count;
        const f_count: f32 = @floatFromInt(self.count);
        self.mean += (value - self.mean) / f_count;
    }
};

const Records = struct {
    const Self = @This();

    map: std.StringHashMap(Record),

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

    pub fn print(self: Self) void {
        std.debug.print("Records\n", .{});
        var entries = self.map.iterator();
        while (entries.next()) |entry| {
            const city_name = entry.key_ptr.*;
            const city_data = entry.value_ptr.*;
            std.debug.print(
                "{s} : ({}, {}, {}, {})\n",
                .{
                    city_name,
                    city_data.min,
                    city_data.mean,
                    city_data.max,
                    city_data.count,
                },
            );
        } else {}
    }

    pub fn merge(self: *Self, records: Self) !void {
        var incoming_entries = records.map.iterator();
        while (incoming_entries.next()) |entry| {
            const city_name = entry.key_ptr.*;
            const city_data = entry.value_ptr.*;

            if (self.map.getPtr(city_name)) |own_record| {
                own_record.min = @min(own_record.min, city_data.min);
                own_record.max = @max(own_record.max, city_data.max);
                own_record.addToMean(city_data.mean, city_data.count);
            } else {
                const own_key = try self.map.allocator.alloc(u8, city_name.len);
                @memcpy(own_key, city_name);

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
    master_records: *Records,
) !void {
    var records_arena = std.heap.ArenaAllocator.init(std.heap.page_allocator);
    defer records_arena.deinit();

    var records = Records.init(records_arena.allocator());
    defer records.deinit();

    var buffer: [BUF_SIZE]u8 = undefined;
    const file = std.fs.cwd().openFile(
        file_path,
        .{
            .mode = .read_only,
        },
    ) catch |err| {
        std.debug.print("ERR: {any}", .{err});
        @panic("Could not open file.");
    };
    defer file.close();

    var reader_wrapper = file.reader(io, &buffer);
    const reader: *std.Io.Reader = &reader_wrapper.interface;

    var index: u32 = 0;
    while (reader.takeDelimiterExclusive('\n')) |line| {
        reader.toss(1);

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

        index += 1;
    } else |err| {
        switch (err) {
            error.EndOfStream => {},
            else => return error.ReaderError,
        }
    }

    try master_records.merge(records);
}

pub fn main() !void {
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    const alloc = gpa.allocator();

    var args = try std.process.argsWithAllocator(alloc);
    defer args.deinit();

    _ = args.next();

    const file_path = args.next() orelse @panic("File path not provided.");

    var records_arena = std.heap.ArenaAllocator.init(std.heap.page_allocator);
    defer records_arena.deinit();
    var thread_safe = std.heap.ThreadSafeAllocator{ .child_allocator = records_arena.allocator() };
    var records = Records.init(thread_safe.allocator());

    const concurrent = true;
    if (concurrent) {
        // var threaded = std.Io.Threaded.init_single_threaded;
        var threaded = std.Io.Threaded.init(gpa.allocator());
        defer threaded.deinit();
        const io = threaded.io();

        // const thread_limit = io.concurrent_limit.toInt() orelse return error.NoThreadLimit;
        // try readFileChunk(io, file_path);

        var task = try io.concurrent(readFileChunk, .{
            io,
            file_path,
            &records,
        });

        try task.await(io);
    } else {
        var threaded = std.Io.Threaded.init_single_threaded;
        const io = threaded.io();
        try readFileChunk(io, file_path, &records);
    }

    records.print();
}
