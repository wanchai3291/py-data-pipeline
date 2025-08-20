from importlib.metadata import version, PackageNotFoundError
from packaging.version import parse

from pyeqxextension.common.result import FunctionExecuteResult


class PackageVersionNotMatchException(Exception):
    def __init__(self, name: str, installed_version: str, desired_version: str):
        self.name = name

        super().__init__(
            f"Package {name} found, version {installed_version}, but it is higher than desired version {desired_version}. Please check the package version."
        )


class PackageNotFound(Exception):
    def __init__(self, name: str, version: str, path: str):
        self.name = name
        self.version = version
        self.path = path

        super().__init__(
            f"Package {name} ({version}) not found. Please install via pip. (pip3 install --upgrade {path})"
        )


def check_dependencies(packages: dict[str, dict[str, str]]):
    for name, package in packages.items():
        check_result = __check_package(
            name=name, package_version=package["version"], package_path=package["path"]
        )

        if not check_result.is_success:
            return FunctionExecuteResult(error=check_result.error)

    return FunctionExecuteResult(data=True)


def __check_package(
    name: str,
    package_version: str,
    package_path: str,
):
    try:
        installed_version = version(name)
        parsed_installed_version = parse(installed_version)
        parsed_desired_version = parse(package_version)

        if parsed_installed_version < parsed_desired_version:
            print(
                f"Package {name} found, but version {installed_version} is lower than desired {package_version}. (pip3 install --upgrade {package_path})"
            )
            return FunctionExecuteResult(data=True)
        else:
            print(f"Package {name} found, version {installed_version}")
            if parsed_installed_version > parsed_desired_version:
                return FunctionExecuteResult(
                    error=PackageVersionNotMatchException(
                        name=name,
                        installed_version=installed_version,
                        desired_version=parsed_desired_version,
                    )
                )

            return FunctionExecuteResult(data=False)
    except PackageNotFoundError:
        return FunctionExecuteResult(
            error=PackageNotFound(name=name, version=package_version, path=package_path)
        )
