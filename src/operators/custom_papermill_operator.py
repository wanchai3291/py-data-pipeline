from pathlib import Path
import os
import subprocess
import sys

from airflow.models import BaseOperator
from airflow.utils.context import Context

import papermill as pm


# Create a simple NoteBook class to replace the problematic import
class NoteBook:
    def __init__(self, url: str, parameters: dict = None):
        self.url = url
        self.parameters = parameters or {}


class CustomPapermillOperator(BaseOperator):
    template_fields = ["parameters"]
    template_fields_renderers = {"parameters": "json"}
    
    __base_dir: str
    __project_desc: str
    __is_kernel_initialized: bool = False
    __dependencies: list = None

    def __init__(
        self,
        input_nb: str,
        output_nb: str,
        parameters: dict = None,
        kernel_name: str = "python3",
        language_name: str = "python",
        base_dir: str = None,
        project_desc: str = None,
        is_kernel_initialized: bool = False,
        dependencies: list = None,
        *args,
        **kwargs,
    ):
        self.__base_dir = base_dir
        self.__project_desc = project_desc
        self.__is_kernel_initialized = is_kernel_initialized
        self.__dependencies = dependencies or []
        
        super().__init__(*args, **kwargs)
        self.input_nb = input_nb
        self.output_nb = output_nb
        self.parameters = parameters or {}
        self.kernel_name = kernel_name
        self.language_name = language_name

    def execute(self, context: Context):
        self.log.info(f"Executing notebook: {self.input_nb}")
        self.log.info(f"Output notebook: {self.output_nb}")
        
        # Convert strings to NoteBook objects if needed (mimicking original behavior)
        if not isinstance(self.input_nb, NoteBook):
            self.input_nb = NoteBook(url=self.input_nb, parameters=self.parameters)
        if not isinstance(self.output_nb, NoteBook):
            self.output_nb = NoteBook(url=self.output_nb)

        self.__get_venv()

        if not self.__is_kernel_initialized:
            self.__create_venv_and_kernel_execution()

        if self.__dependencies:
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
        
        self.log.info("Notebook execution completed successfully")

    def __get_venv(self):
        try:
            root_path = os.path.join(str(Path(self.__base_dir).parent), "venv-projects")

            if not os.path.exists(root_path):
                os.mkdir(root_path)

            self.venv_path = os.path.join(root_path, self.kernel_name)
            self.venv_python_executable = os.path.join(self.venv_path, "bin", "python")
            self.log.info(f"Virtual environment path: {self.venv_path}")
        except Exception as e:
            self.log.error("Failed to get virtual environment.")
            self.log.error(str(e))
            raise

    def __create_venv_and_kernel_execution(self):
        try:
            self.__create_venv()
            self.__install_ipykernel()
            self.__create_kernel()
        except Exception as e:
            self.log.error("Failed to create virtual environment and kernel.")
            self.log.error(str(e))
            raise

    def __create_venv(self):
        if not os.path.exists(self.venv_path):
            self.log.info(f"Creating virtual environment: {self.venv_path}")
            subprocess.run([sys.executable, "-m", "venv", self.venv_path])
            return

        self.log.info(f"Virtual environment already exists: {self.venv_path}")

        activate_python_path = os.path.join(self.venv_path, "bin", "activate")
        if os.path.exists(activate_python_path):
            self.log.info(f"Virtual environment at {self.venv_path} is already up-to-date.")
            return

        self.log.info(f"Upgrading virtual environment at {self.venv_path}")
        subprocess.check_call([sys.executable, "-m", "venv", "--upgrade", self.venv_path])

    def __install_ipykernel(self):
        try:
            self.log.info("Installing ipykernel in virtual environment")
            subprocess.check_call(
                [self.venv_python_executable, "-m", "pip", "install", "ipykernel"]
            )
        except subprocess.CalledProcessError as e:
            self.log.error("Failed to install ipykernel in the virtual environment")
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
            self.log.info(
                f"Kernel '{self.kernel_name}' registered successfully with display name: Python 3 ({self.__project_desc})"
            )
        except subprocess.CalledProcessError as e:
            self.log.error("Failed to register the kernel")
            raise e

    def __install_dependencies(self):
        if not self.__dependencies:
            self.log.info("No dependencies to install")
            return
            
        try:
            self.log.info(f"Installing dependencies: {self.__dependencies}")
            subprocess.check_call(
                [self.venv_python_executable, "-m", "pip", "install", "--upgrade"] + self.__dependencies
            )
            self.log.info(
                f"Installed ({self.__dependencies}) in the virtual environment successfully"
            )
        except subprocess.CalledProcessError as e:
            self.log.error("Failed to install packages in the virtual environment")
            raise e