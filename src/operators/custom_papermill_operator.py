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
        # Always use python3 kernel to avoid kernel creation issues
        self.kernel_name = "python3"
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

        # Install the current project in the virtual environment
        self.__install_current_project()

        # Execute notebook directly with the virtual environment's Python
        self.__execute_with_venv()
        
        self.log.info("Notebook execution completed successfully")

    def __get_venv(self):
        try:
            root_path = os.path.join(str(Path(self.__base_dir).parent), "venv-projects")

            if not os.path.exists(root_path):
                os.mkdir(root_path)

            # Use a simpler venv name to avoid the complex kernel naming
            venv_name = f"venv-{self.__project_desc.replace(' ', '-').replace('(', '').replace(')', '').lower()}"
            self.venv_path = os.path.join(root_path, venv_name)
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
            # Skip kernel creation - just use python3
            self.log.info("Skipping custom kernel creation, using default python3 kernel")
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
                [self.venv_python_executable, "-m", "pip", "install", "--upgrade", "--quiet", "ipykernel"],
                stderr=subprocess.DEVNULL
            )
        except subprocess.CalledProcessError as e:
            self.log.error("Failed to install ipykernel in the virtual environment")
            raise e

    def __create_kernel(self):
        try:
            # Skip custom kernel creation due to permission/file issues
            # Just use the default python3 kernel with our virtual environment
            self.log.info("Skipping custom kernel creation, using python3 kernel with virtual environment")
            self.kernel_name = "python3"
            
        except Exception as e:
            self.log.error("Failed to register the kernel")
            # Don't fail the entire task, just use default kernel
            self.log.warning("Falling back to default python3 kernel")
            self.kernel_name = "python3"

    def __install_dependencies(self):
        if not self.__dependencies:
            self.log.info("No dependencies to install")
            return
            
        try:
            self.log.info(f"Installing dependencies: {self.__dependencies}")
            subprocess.check_call(
                [self.venv_python_executable, "-m", "pip", "install", "--upgrade", "--quiet"] + self.__dependencies,
                stderr=subprocess.DEVNULL
            )
            self.log.info(
                f"Installed ({self.__dependencies}) in the virtual environment successfully"
            )
        except subprocess.CalledProcessError as e:
            self.log.error("Failed to install packages in the virtual environment")
            raise e

    def __install_current_project(self):
        """Install the current project in editable mode to make local modules available"""
        try:
            # Install the project in editable mode so 'app' module can be imported
            if os.path.exists(os.path.join(self.__base_dir, "setup.py")):
                self.log.info("Installing current project with setup.py")
                subprocess.check_call(
                    [self.venv_python_executable, "-m", "pip", "install", "-e", self.__base_dir, "--quiet"],
                    stderr=subprocess.DEVNULL
                )
            elif os.path.exists(os.path.join(self.__base_dir, "pyproject.toml")):
                self.log.info("Installing current project with pyproject.toml")
                subprocess.check_call(
                    [self.venv_python_executable, "-m", "pip", "install", "-e", self.__base_dir, "--quiet"],
                    stderr=subprocess.DEVNULL
                )
            else:
                self.log.info("No setup.py or pyproject.toml found, adding base_dir to PYTHONPATH")
        except subprocess.CalledProcessError as e:
            self.log.warning(f"Failed to install current project: {e}")
            self.log.info("Will rely on PYTHONPATH instead")

    def __execute_with_venv(self):
        """Execute notebook with proper virtual environment setup"""
        # Set up environment to use the virtual environment
        env = os.environ.copy()
        env['PATH'] = f"{os.path.dirname(self.venv_python_executable)}:{env.get('PATH', '')}"
        env['PYTHONPATH'] = f"{self.__base_dir}/src:{self.__base_dir}:{env.get('PYTHONPATH', '')}"
        env['VIRTUAL_ENV'] = self.venv_path
        
        # Change to the base directory so relative imports work
        original_cwd = os.getcwd()
        try:
            os.chdir(self.__base_dir)
            
            self.log.info(f"Executing with python3 kernel and virtual environment: {self.venv_path}")
            self.log.info(f"PYTHONPATH: {env['PYTHONPATH']}")
            self.log.info(f"Working directory: {os.getcwd()}")
            
            pm.execute_notebook(
                self.input_nb.url,
                self.output_nb.url,
                parameters=self.input_nb.parameters,
                progress_bar=False,
                report_mode=True,
                log_output=True,
                kernel_name="python3",
                language=self.language_name,
                env=env,
            )
        finally:
            os.chdir(original_cwd)