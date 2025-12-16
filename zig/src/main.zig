const std = @import("std");

const Record = struct {
    min: f32 = std.math.floatMax(f32),
    max: f32 = std.math.floatMax(f32),
    mean: f32 = 0,
    count: u32 = 0,
};

fn readFileChunk(
    file: std.fs.File,
    seek: u64,
    map_alloc: std.mem.Allocator,
) !std.StringHashMap(Record) {
    var map = std.StringHashMap(Record).init(map_alloc);

    // TODO : Optimal buffer size ?
    var buffer: [1028]u8 = undefined;
    try file.seekTo(seek);
    var reader_wrapper = file.reader(&buffer);
    const reader: *std.Io.Reader = &reader_wrapper.interface;

    try reader.readSliceAll(&buffer);

    var index: u32 = 0;
    while (reader.takeDelimiterExclusive('\n')) |line| {
        reader.toss(1);

        var line_iter = std.mem.splitAny(u8, line, ";");
        const city_name = line_iter.next() orelse return error.ParseError;
        const city_temp = brk: {
            const str = line_iter.next() orelse return error.ParseError;

            break :brk try std.fmt.parseFloat(f32, str);
        };

        const key = try map_alloc.alloc(u8, city_name.len);
        @memmove(key, city_name);
        const entry = try map.getOrPut((key));
        if (entry.found_existing) {
            entry.value_ptr.*.max = @max(entry.value_ptr.max, city_temp);
            entry.value_ptr.*.min = @min(entry.value_ptr.min, city_temp);
            entry.value_ptr.*.count += 1;
            entry.value_ptr.*.mean += (city_temp - entry.value_ptr.*.mean) / @as(f32, @floatFromInt(entry.value_ptr.*.count));
        } else {
            entry.value_ptr.*.max = city_temp;
            entry.value_ptr.*.min = city_temp;
            entry.value_ptr.*.count = 1;
            entry.value_ptr.*.mean = city_temp;
        }

        index += 1;
    } else |err| {
        switch (err) {
            error.EndOfStream => {},
            else => return error.ReaderError,
        }
    }

    return map;
}

pub fn main() !void {
    // - Split file in chunks
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    const alloc = gpa.allocator();

    var args = try std.process.argsWithAllocator(alloc);
    defer args.deinit();

    // Ignore program name
    _ = args.next();

    const file_path = args.next() orelse @panic("File path not provided.");

    const file = std.fs.cwd().openFile(
        file_path,
        .{ .mode = .read_only },
    ) catch |err| {
        std.debug.print("ERR: {any}", .{err});
        @panic("Could not open file.");
    };
    defer file.close();

    // const file_stat = file.stat() catch {
    //     @panic("Could not stat file.");
    // };

    // file_stat.size // SPLIT FILE & CHUNKS

    // - Read chunks in parallel
    // - Compute min, mean and max temps per chunk
    var map_arena = std.heap.ArenaAllocator.init(std.heap.page_allocator);
    defer map_arena.deinit();
    var map = try readFileChunk(file, 0, map_arena.allocator());
    defer map.deinit();

    var entries = map.iterator();
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

    // - Aggregate all the chunks.

    // - Print JSON
}
