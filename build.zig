const std = @import("std");

pub fn build(b: *std.Build) void {
    const target = b.standardTargetOptions(.{});
    const optimize = b.standardOptimizeOption(.{});

    const parquet_mod = b.createModule(.{
        .root_source_file = b.path("src/parquet/parquet.zig"),
        .target = target,
        .optimize = optimize,
    });

    const dataframe_mod = b.createModule(.{
        .root_source_file = b.path("src/dataframe/dataframe.zig"),
        .target = target,
        .optimize = optimize,
        .imports = &.{
            .{ .name = "parquet", .module = parquet_mod },
        },
    });

    const exe = b.addExecutable(.{
        .name = "teddy",
        .root_module = b.createModule(.{
            .root_source_file = b.path("src/main.zig"),
            .target = target,
            .optimize = optimize,
            .imports = &.{
                .{ .name = "teddy", .module = dataframe_mod },
            },
        }),
    });

    b.installArtifact(exe);

    const run_step = b.step("run", "Run the app");

    const run_cmd = b.addRunArtifact(exe);
    run_step.dependOn(&run_cmd.step);

    run_cmd.step.dependOn(b.getInstallStep());

    if (b.args) |args| {
        run_cmd.addArgs(args);
    }

    // Benchmark executable (Phase 14.0). Mirrors the `run` wiring. NOT part of
    // the `test` step (timing is noisy; correctness is the gate). Real numbers
    // require -Doptimize=ReleaseFast.
    const bench_exe = b.addExecutable(.{
        .name = "teddy-bench",
        .root_module = b.createModule(.{
            .root_source_file = b.path("src/bench/main.zig"),
            .target = target,
            .optimize = optimize,
            .imports = &.{
                .{ .name = "teddy", .module = dataframe_mod },
            },
        }),
    });

    b.installArtifact(bench_exe);

    const bench_step = b.step("bench", "Run the benchmark harness (use -Doptimize=ReleaseFast for real numbers)");
    const bench_cmd = b.addRunArtifact(bench_exe);
    bench_cmd.step.dependOn(b.getInstallStep());
    bench_step.dependOn(&bench_cmd.step);
    if (b.args) |args| {
        bench_cmd.addArgs(args);
    }

    const mod_tests = b.addTest(.{
        .root_module = b.createModule(.{
            .root_source_file = b.path("src/dataframe/tests.zig"),
            .target = target,
            .optimize = optimize,
            .imports = &.{
                .{ .name = "parquet", .module = parquet_mod },
                .{ .name = "dataframe", .module = dataframe_mod },
            },
        }),
    });

    const run_mod_tests = b.addRunArtifact(mod_tests);

    const exe_tests = b.addTest(.{
        .root_module = exe.root_module,
    });

    const run_exe_tests = b.addRunArtifact(exe_tests);

    const parquet_tests = b.addTest(.{
        .root_module = b.createModule(.{
            .root_source_file = b.path("src/parquet/tests.zig"),
            .target = target,
            .optimize = optimize,
        }),
    });

    const run_parquet_tests = b.addRunArtifact(parquet_tests);

    const test_step = b.step("test", "Run tests");
    test_step.dependOn(&run_mod_tests.step);
    test_step.dependOn(&run_exe_tests.step);
    test_step.dependOn(&run_parquet_tests.step);
}
