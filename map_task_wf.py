import os
from dataclasses import dataclass, field
from flytekit.types.file import FlyteFile
from flytekit import task, workflow, ImageSpec, map_task


flytekit_hash = "5bf0acd22623f02293a75d41952dd4ec26e025ae"
flytekit = f"git+https://github.com/flyteorg/flytekit.git@{flytekit_hash}"

image = ImageSpec(
    packages=[
        flytekit,
        "pandas",
        "pydantic>2",
        "pyarrow"
    ],
    apt_packages=["git"],
    registry="futureoutlier",
    env={"FLYTE_SDK_LOGGING_LEVEL": "20"},
)

image = "futureoutlier/flytekit:6oCREy62XKWzgJswOZyZUg"

@dataclass
class MyDataClass:
    my_ints: list[int] = field(default_factory=lambda: [-1, 0, 1, 2, 3])
    my_files: list[FlyteFile] = field(default_factory=lambda: [FlyteFile("s3://my-s3-bucket/s3_flyte_dir/example.txt"),
                                                               FlyteFile("s3://my-s3-bucket/s3_flyte_dir/example.txt"),
                                                               FlyteFile("s3://my-s3-bucket/s3_flyte_dir/example.txt")])

@task(container_image=image)
def print_int(my_int: int) -> int:
    print(f"my_int: {my_int}")
    return my_int

@task(container_image=image)
def print_file(my_file: FlyteFile) -> str:
    with open(my_file, "r") as f:
        content = f.read()
        print(f"my_file: {content}")
        return content

@workflow
def map_wf(dc: MyDataClass) -> list[str]:
    map_task(print_int)(my_int=dc.my_ints)
    str_list = map_task(print_file)(my_file=dc.my_files)
    return str_list

if __name__ == "__main__":
    from flytekit.clis.sdk_in_container import pyflyte
    from click.testing import CliRunner

    runner = CliRunner()
    path = os.path.realpath(__file__)
    input_val = '{}'
    result = runner.invoke(pyflyte.main, ["run", path, "map_wf", "--dc", input_val])
    print("Local Execution: ", result.output)
    result = runner.invoke(pyflyte.main, ["run", "--remote", path, "map_wf", "--dc", input_val])
    print("Remote Execution: ", result.output)
