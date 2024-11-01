import logging
from dataclasses import dataclass
from typing import Tuple
from flytekit import ContainerTask, kwtypes, workflow
logger = logging.getLogger(__file__)

@dataclass
class DC:
    a: int = 1
    b: bool = True
    c: float = 1.1
    d: str = "string"

@workflow
def wf(dc: DC) -> Tuple[int, bool, float, str]:
    return python_return_same_values(a=dc.a, b=dc.b, c=dc.c, d=dc.d)

python_return_same_values = ContainerTask(
    name="python_return_same_values",
    input_data_dir="/var/inputs",
    output_data_dir="/var/outputs",
    inputs=kwtypes(a=int, b=bool, c=float, d=str),
    outputs=kwtypes(a=int, b=bool, c=float, d=str),
    image="futureoutlier/rawcontainer:0320",
    command=[
        "python",
        "return_same_value.py",
        "{{.inputs.a}}",
        "{{.inputs.b}}",
        "{{.inputs.c}}",
        "{{.inputs.d}}",
        "/var/outputs",
    ],
)

if __name__ == "__main__":
    from flytekit.clis.sdk_in_container import pyflyte
    from click.testing import CliRunner
    import os

    runner = CliRunner()
    path = os.path.realpath(__file__)
    input_val = '{"a": 1, "b": true, "c": 1.0, "d": "string"}'

    result = runner.invoke(pyflyte.main,
                           ["run", path, "wf", "--dc", input_val])
    print("Local Execution: ", result.output)

    result = runner.invoke(pyflyte.main,
                           ["run", "--remote", path, "wf", "--dc", input_val])
    print("Remote Execution: ", result.output)
