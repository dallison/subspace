load("@bazel_tools//tools/build_defs/repo:http.bzl", "http_archive")

http_archive(
    name = "platforms",
    urls = [
        "https://mirror.bazel.build/github.com/bazelbuild/platforms/releases/download/0.0.6/platforms-0.0.6.tar.gz",
        "https://github.com/bazelbuild/platforms/releases/download/0.0.6/platforms-0.0.6.tar.gz",
    ],
    sha256 = "5308fc1d8865406a49427ba24a9ab53087f17f5266a7aabbfc28823f3916e1ca",
)

http_archive(
  name = "bazel_skylib",
  urls = ["https://github.com/bazelbuild/bazel-skylib/releases/download/1.2.1/bazel-skylib-1.2.1.tar.gz"],
  sha256 = "f7be3474d42aae265405a592bb7da8e171919d74c16f082a5457840f06054728",
)

#http_archive(
    #name = "com_google_absl",
    #urls = ["https://github.com/abseil/abseil-cpp"],
    #sha256 = "4f356a07b9ec06ef51f943928508566e992f621ed5fa4dd588865d7bed1284cd",
#)

http_archive(
    name = "com_google_protobuf",
    urls = ["https://github.com/protocolbuffers/protobuf/releases/download/v21.12/protobuf-all-21.12.tar.gz"],
    strip_prefix = "protobuf-21.12",
)

load("@com_google_protobuf//:protobuf_deps.bzl", "protobuf_deps")

http_archive(
    name = "rules_cc",
    urls = ["https://github.com/bazelbuild/rules_cc/releases/download/0.0.4/rules_cc-0.0.4.tar.gz"],
    sha256 = "af6cc82d87db94585bceeda2561cb8a9d55ad435318ccb4ddfee18a43580fb5d",
    strip_prefix = "rules_cc-0.0.4",
)

http_archive(
    name = "rules_pkg",
    urls = [
        "https://mirror.bazel.build/github.com/bazelbuild/rules_pkg/releases/download/0.8.0/rules_pkg-0.8.0.tar.gz",
        "https://github.com/bazelbuild/rules_pkg/releases/download/0.8.0/rules_pkg-0.8.0.tar.gz",
    ],
    sha256 = "eea0f59c28a9241156a47d7a8e32db9122f3d50b505fae0f33de6ce4d9b61834",
)
load("@rules_pkg//:deps.bzl", "rules_pkg_dependencies")
rules_pkg_dependencies()

http_archive(
    name = "zlib",
    build_file = "@com_google_protobuf//:third_party/zlib.BUILD",
    sha256 = "d14c38e313afc35a9a8760dadf26042f51ea0f5d154b0630a31da0540107fb98",
    strip_prefix = "zlib-1.2.13",
    urls = [
       "https://github.com/madler/zlib/releases/download/v1.2.13/zlib-1.2.13.tar.xz",
       "https://zlib.net/zlib-1.2.13.tar.xz",
   ],
)

protobuf_deps()

http_archive(
  name = "com_google_googletest",
  urls = ["https://github.com/google/googletest/archive/5ab508a01f9eb089207ee87fd547d290da39d015.zip"],
  strip_prefix = "googletest-5ab508a01f9eb089207ee87fd547d290da39d015",
)

http_archive(
  name = "toolbelt",
  urls = ["https://github.com/dallison/cpp_toolbelt/archive/refs/tags/1.1.2.tar.gz"],
  strip_prefix = "cpp_toolbelt-1.1.2",
  sha256 = "1d9a8b4140ff7db1c579a0ba7f745bcb8464159388bcea2e4b3b8b5266c5e9d7"
)

# For local debugging of toolbelt coroutine library.
# local_repository(
#     name = "toolbelt",
#     path = "../cpp_toolbelt",
# )

http_archive(
  name = "coroutines",
  urls = ["https://github.com/dallison/co/archive/refs/tags/1.3.4.tar.gz"],
  strip_prefix = "co-1.3.4",
  sha256 = "00b036fcecd0256356bc09b47b049e29f6df34dc97792143630df83591de2c1b"
)

# For local debugging of co coroutine library.
# local_repository(
#     name = "coroutines",
#     path = "../co",
# )

# Bazel python rules.
http_archive(
  name = "rules_python",
  sha256 = "29a801171f7ca190c543406f9894abf2d483c206e14d6acbd695623662320097",
  strip_prefix = "rules_python-0.18.1",
  url = "https://github.com/bazelbuild/rules_python/releases/download/0.18.1/rules_python-0.18.1.tar.gz",
)

# Python toolchains
load("@rules_python//python:repositories.bzl", "python_register_toolchains")

python_register_toolchains(
    name = "python_default",
    ignore_root_user_error = True,
    # Available versions are listed in @rules_python//python:versions.bzl.
    python_version = "3.11.1",
)

load("@python_default//:defs.bzl", "interpreter")
load("@rules_python//python:pip.bzl", "pip_parse")

pip_parse(
  python_interpreter_target = interpreter,
)

http_archive(
  name = "pybind11",
  build_file = "@//bzl/third_party:pybind11.BUILD",
  strip_prefix = "pybind11-2.10.0",
  sha256 = "225df6e6dea7cea7c5754d4ed954e9ca7c43947b849b3795f87cb56437f1bd19",
  urls = ["https://github.com/pybind/pybind11/archive/refs/tags/v2.10.0.zip"],
)
