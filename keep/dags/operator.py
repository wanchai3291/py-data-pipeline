from logging import Logger
from pathlib import Path
import os
import subprocess
import sys

from airflow.providers.papermill.operators.papermill import NoteBook, PapermillOperator
from airflow.utils.context import Context

import papermill as pm


class CustomPapermillOperator(PapermillOperator):
    __logger: Logger

    __project_desc: str
    __base_dir: str

    __is_kernel_initialized: bool = False
    __dependencies: list = None

    def __init__(
        self,
        logger: Logger,
        base_dir: str,
        project_desc: str,
        is_kernel_initialized: bool = False,
        dependencies: list = None,
        *args,
        **kwargs,
    ):
        self.__logger = logger
        self.__base_dir = base_dir
        self.__project_desc = project_desc
        self.__is_kernel_initialized = is_kernel_initialized
        self.__dependencies = dependencies

        super().__init__(*args, **kwargs)

    def execute(self, context: Context):
        if not isinstance(self.input_nb, NoteBook):
            self.input_nb = NoteBook(url=self.input_nb, parameters=self.parameters)
        if not isinstance(self.output_nb, NoteBook):
            self.output_nb = NoteBook(url=self.output_nb)
        self.inlets.append(self.input_nb)
        self.outlets.append(self.output_nb)

        self.__get_venv()

        if not self.__is_kernel_initialized:
            self.__create_venv_and_kernel_execution()

        self.__install_dependencies()

        pm.execute_notebook(
            self.input_nb.url,
            self.output_nb.url,
            parameters=self.input_nb.parameters,
            progress_bar=False,
            report_mode=True,
            log_output=True,
            kernel_name=self.kernel_name,
            language=self.language_name,
        )

    def __get_venv(self):
        try:
            root_path = os.path.join(str(Path(self.__base_dir).parent), "venv-projects")

            if not os.path.exists(root_path):
                os.mkdir(root_path)

            self.venv_path = os.path.join(root_path, self.kernel_name)
            self.venv_python_executable = os.path.join(self.venv_path, "bin", "python")
        except Exception as e:
            self.__logger.error("Failed to get virtual environment.")
            self.__logger.error(e)
            raise

    def __create_venv_and_kernel_execution(self):
        try:
            self.__create_venv()
            self.__install_ipykernel()
            self.__create_kernel()
        except Exception as e:
            self.__logger.error("Failed to create virtual environment and kernel.")
            self.__logger.error(e)
            raise

    def __create_venv(self):
        if not os.path.exists(self.venv_path):
            self.__logger.info(f"Creating virtual environment: {self.venv_path}")
            subprocess.run([sys.executable, "-m", "venv", self.venv_path])
            return

        self.__logger.info(f"Virtual environment already exists: {self.venv_path}")

        # Check if the Python3 binary exists in the virtual environment
        activate_python_path = os.path.join(self.venv_path, "bin", "activate")
        if os.path.exists(activate_python_path):
            self.__logger.info(f"Virtual environment at {self.venv_path} is already up-to-date.")
            return

        self.__logger.info(f"Upgrading virtual environment at {self.venv_path}")
        subprocess.check_call([sys.executable, "-m", "venv", "--upgrade", self.venv_path])

    def __install_ipykernel(self):
        try:
            subprocess.check_call(
                [self.venv_python_executable, "-m", "pip", "install", "ipykernel"]
            )
        except subprocess.CalledProcessError as e:
            self.__logger.error(
                "Failed to install ipykernel in the virtual environment"
            )
            raise e

    def __create_kernel(self):
        try:
            subprocess.check_call(
                [
                    self.venv_python_executable,
                    "-m",
                    "ipykernel",
                    "install",
                    "--user",
                    "--name",
                    self.kernel_name,
                    "--display-name",
                    f"Python 3 ({self.__project_desc})",
                ]
            )

            self.__logger.info(
                f"Kernel '{self.kernel_name}' registered successfully with display name: Python 3 ({self.__project_desc})"
            )
        except subprocess.CalledProcessError as e:
            self.__logger.error("Failed to register the kernel")
            raise e

    def __install_dependencies(self):
        try:
            subprocess.check_call(
                [self.venv_python_executable, "-m", "pip", "install", "--upgrade"] + self.__dependencies
            )
            self.__logger.info(
                f"Installed ({self.__dependencies}) in the virtual environment successful"
            )
        except subprocess.CalledProcessError as e:
            self.__logger.error(
                "Failed to install packages in the virtual environment"
            )
            raise e
