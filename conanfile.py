from conan import ConanFile
from conan.tools.cmake import CMake, cmake_layout


class RakNetPythonConan(ConanFile):
    name = "raknet-python"
    settings = "os", "compiler", "build_type", "arch"
    generators = "CMakeToolchain", "CMakeDeps"
    default_options = {"raknet/*:shared": False}

    def layout(self):
        cmake_layout(self)

    def requirements(self):
        self.requires("raknet/4.081-mojang")
        self.requires("nanobind/2.13.0")

    def build(self):
        cmake = CMake(self)
        cmake.configure()
        cmake.build()

    def package(self):
        cmake = CMake(self)
        cmake.install()
