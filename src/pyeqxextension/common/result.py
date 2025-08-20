from typing import Generic, Optional, TypeVar

TResultData = TypeVar("TResultData")


class FunctionExecuteResultBase(Generic[TResultData]):
    def __init__(
        self,
        data: TResultData = None,
        is_success: bool = False,
        error: Exception = None,
    ):
        self.data: TResultData = data
        self.is_success = is_success
        self.error = error


class FunctionExecuteResult(FunctionExecuteResultBase[TResultData]):
    def __init__(
        self, data: TResultData | None = None, error: Optional[Exception] = None
    ):
        if error is None:
            super().__init__(data=data, is_success=True, error=None)
        else:
            super().__init__(data=None, is_success=False, error=error)
