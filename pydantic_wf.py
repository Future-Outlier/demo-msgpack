from pydantic import BaseModel, Field
from typing import Dict, List

from flytekit.types.schema import FlyteSchema
from flytekit.types.structured import StructuredDataset
from flytekit.types.file import FlyteFile
from flytekit.types.directory import FlyteDirectory
from flytekit import task, workflow, ImageSpec
from enum import Enum
import pandas as pd
import os

image = "futureoutlier/flytekit:6oCREy62XKWzgJswOZyZUg"

class Status(Enum):
    PENDING = "pending"
    APPROVED = "approved"
    REJECTED = "rejected"

class InnerBM(BaseModel):
    a: int = -1
    b: float = 2.1
    c: str = "Hello, Flyte"
    d: bool = False
    e: List[int] = Field(default_factory=lambda: [0, 1, 2, -1, -2])
    f: List[FlyteFile] = Field(default_factory=lambda: [FlyteFile("s3://my-s3-bucket/s3_flyte_dir/example.txt")])
    g: List[List[int]] = Field(default_factory=lambda: [[0], [1], [-1]])
    h: List[Dict[int, bool]] = Field(default_factory=lambda: [{0: False}, {1: True}, {-1: True}])
    i: Dict[int, bool] = Field(default_factory=lambda: {0: False, 1: True, -1: False})
    j: Dict[int, FlyteFile] = Field(default_factory=lambda: {0: FlyteFile("s3://my-s3-bucket/s3_flyte_dir/example.txt"),
                                                             1: FlyteFile("s3://my-s3-bucket/s3_flyte_dir/example.txt"),
                                                             -1: FlyteFile("s3://my-s3-bucket/s3_flyte_dir/example.txt")})
    k: Dict[int, List[int]] = Field(default_factory=lambda: {0: [0, 1, -1]})
    l: Dict[int, Dict[int, int]] = Field(default_factory=lambda: {1: {-1: 0}})
    m: dict = Field(default_factory=lambda: {"key": "value"})
    n: FlyteFile = Field(default_factory=lambda: FlyteFile("s3://my-s3-bucket/s3_flyte_dir/example.txt"))
    o: FlyteDirectory = Field(default_factory=lambda: FlyteDirectory("s3://my-s3-bucket/s3_flyte_dir"))
    enum_status: Status = Status.PENDING
    sd: StructuredDataset = Field(default_factory=lambda: StructuredDataset(
                                    uri="s3://my-s3-bucket/s3_flyte_dir/df.parquet",
                                    file_format="parquet"))
    fsc: FlyteSchema = Field(default_factory=lambda: FlyteSchema(
                                remote_path="s3://my-s3-bucket/s3_flyte_dir/df.parquet"))

class BM(BaseModel):
    a: int = -1
    b: float = 2.1
    c: str = "Hello, Flyte"
    d: bool = False
    e: List[int] = Field(default_factory=lambda: [0, 1, 2, -1, -2])
    f: List[FlyteFile] = Field(default_factory=lambda: [FlyteFile("s3://my-s3-bucket/s3_flyte_dir/example.txt")])
    g: List[List[int]] = Field(default_factory=lambda: [[0], [1], [-1]])
    h: List[Dict[int, bool]] = Field(default_factory=lambda: [{0: False}, {1: True}, {-1: True}])
    i: Dict[int, bool] = Field(default_factory=lambda: {0: False, 1: True, -1: False})
    j: Dict[int, FlyteFile] = Field(default_factory=lambda: {0: FlyteFile("s3://my-s3-bucket/s3_flyte_dir/example.txt"),
                                                             1: FlyteFile("s3://my-s3-bucket/s3_flyte_dir/example.txt"),
                                                             -1: FlyteFile("s3://my-s3-bucket/s3_flyte_dir/example.txt")})
    k: Dict[int, List[int]] = Field(default_factory=lambda: {0: [0, 1, -1]})
    l: Dict[int, Dict[int, int]] = Field(default_factory=lambda: {1: {-1: 0}})
    m: dict = Field(default_factory=lambda: {"key": "value"})
    n: FlyteFile = Field(default_factory=lambda: FlyteFile("s3://my-s3-bucket/s3_flyte_dir/example.txt"))
    o: FlyteDirectory = Field(default_factory=lambda: FlyteDirectory("s3://my-s3-bucket/s3_flyte_dir"))
    inner_bm: InnerBM = Field(default_factory=lambda: InnerBM())
    enum_status: Status = Status.PENDING
    sd: StructuredDataset = Field(default_factory=lambda: StructuredDataset(
        uri="s3://my-s3-bucket/s3_flyte_dir/df.parquet",
        file_format="parquet"))
    fsc: FlyteSchema = Field(default_factory=lambda: FlyteSchema(
        remote_path="s3://my-s3-bucket/s3_flyte_dir/df.parquet"))


@task(container_image=image)
def t_bm(bm: BM) -> BM:
    return bm

@task(container_image=image)
def t_inner(inner_bm: InnerBM):
    assert isinstance(inner_bm, InnerBM)

    expected_file_content = "Default content"

    # f: List[FlyteFile]
    for ff in inner_bm.f:
        assert isinstance(ff, FlyteFile)
        with open(ff, "r") as f:
            assert f.read() == expected_file_content
    # j: Dict[int, FlyteFile]
    for _, ff in inner_bm.j.items():
        assert isinstance(ff, FlyteFile)
        with open(ff, "r") as f:
            assert f.read() == expected_file_content
    # n: FlyteFile
    assert isinstance(inner_bm.n, FlyteFile)
    with open(inner_bm.n, "r") as f:
        assert f.read() == expected_file_content
    # o: FlyteDirectory
    assert isinstance(inner_bm.o, FlyteDirectory)
    assert not inner_bm.o.downloaded
    with open(os.path.join(inner_bm.o, "example.txt"), "r") as fh:
        assert fh.read() == expected_file_content
    assert inner_bm.o.downloaded
    print("Test InnerBM Successfully Passed")
    # enum: Status
    assert inner_bm.enum_status == Status.PENDING

    assert isinstance(inner_bm.sd, StructuredDataset), "sd is not StructuredDataset"
    print("sd:", inner_bm.sd.open(pd.DataFrame).all())

    assert isinstance(inner_bm.fsc, FlyteSchema), "fsc is not FlyteSchema"
    print("fsc: ", inner_bm.fsc.open().all())


@task(container_image=image)
def t_test_all_attributes(a: int, b: float, c: str, d: bool, e: List[int], f: List[FlyteFile], g: List[List[int]],
                          h: List[Dict[int, bool]], i: Dict[int, bool], j: Dict[int, FlyteFile],
                          k: Dict[int, List[int]], l: Dict[int, Dict[int, int]], m: dict,
                          n: FlyteFile, o: FlyteDirectory,
                          enum_status: Status,
                          sd: StructuredDataset,
                          fsc: FlyteSchema,
                          ):
    # Strict type checks for simple types
    assert isinstance(a, int), f"a is not int, it's {type(a)}"
    assert a == -1
    assert isinstance(b, float), f"b is not float, it's {type(b)}"
    assert isinstance(c, str), f"c is not str, it's {type(c)}"
    assert isinstance(d, bool), f"d is not bool, it's {type(d)}"

    # Strict type checks for List[int]
    assert isinstance(e, list) and all(isinstance(i, int) for i in e), "e is not List[int]"

    # Strict type checks for List[FlyteFile]
    assert isinstance(f, list) and all(isinstance(i, FlyteFile) for i in f), "f is not List[FlyteFile]"

    # Strict type checks for List[List[int]]
    assert isinstance(g, list) and all(
        isinstance(i, list) and all(isinstance(j, int) for j in i) for i in g), "g is not List[List[int]]"

    # Strict type checks for List[Dict[int, bool]]
    assert isinstance(h, list) and all(
        isinstance(i, dict) and all(isinstance(k, int) and isinstance(v, bool) for k, v in i.items()) for i in h
    ), "h is not List[Dict[int, bool]]"

    # Strict type checks for Dict[int, bool]
    assert isinstance(i, dict) and all(
        isinstance(k, int) and isinstance(v, bool) for k, v in i.items()), "i is not Dict[int, bool]"

    # Strict type checks for Dict[int, FlyteFile]
    assert isinstance(j, dict) and all(
        isinstance(k, int) and isinstance(v, FlyteFile) for k, v in j.items()), "j is not Dict[int, FlyteFile]"

    # Strict type checks for Dict[int, List[int]]
    assert isinstance(k, dict) and all(
        isinstance(k, int) and isinstance(v, list) and all(isinstance(i, int) for i in v) for k, v in
        k.items()), "k is not Dict[int, List[int]]"

    # Strict type checks for Dict[int, Dict[int, int]]
    assert isinstance(l, dict) and all(
        isinstance(k, int) and isinstance(v, dict) and all(
            isinstance(sub_k, int) and isinstance(sub_v, int) for sub_k, sub_v in v.items())
        for k, v in l.items()), "l is not Dict[int, Dict[int, int]]"

    # Strict type check for a generic dict
    assert isinstance(m, dict), "m is not dict"

    # Strict type check for FlyteFile
    assert isinstance(n, FlyteFile), "n is not FlyteFile"

    # Strict type check for FlyteDirectory
    assert isinstance(o, FlyteDirectory), "o is not FlyteDirectory"

    # # Strict type check for Enum
    assert isinstance(enum_status, Status), "enum_status is not Status"

    assert isinstance(sd, StructuredDataset), "sd is not StructuredDataset"
    print("sd:", sd.open(pd.DataFrame).all())

    assert isinstance(fsc, FlyteSchema), "fsc is not FlyteSchema"
    print("fsc: ", fsc.open().all())

    print("All attributes passed strict type checks.")


@workflow
def wf(bm: BM):
    t_bm(bm=bm)
    t_inner(inner_bm=bm.inner_bm)
    t_test_all_attributes(a=bm.a, b=bm.b, c=bm.c,
                          d=bm.d, e=bm.e, f=bm.f,
                          g=bm.g, h=bm.h, i=bm.i,
                          j=bm.j, k=bm.k, l=bm.l,
                          m=bm.m, n=bm.n, o=bm.o,
                          enum_status=bm.enum_status,
                          sd=bm.sd,
                          fsc=bm.fsc,
                          )

    t_test_all_attributes(a=bm.inner_bm.a, b=bm.inner_bm.b, c=bm.inner_bm.c,
                          d=bm.inner_bm.d, e=bm.inner_bm.e, f=bm.inner_bm.f,
                          g=bm.inner_bm.g, h=bm.inner_bm.h, i=bm.inner_bm.i,
                          j=bm.inner_bm.j, k=bm.inner_bm.k, l=bm.inner_bm.l,
                          m=bm.inner_bm.m, n=bm.inner_bm.n, o=bm.inner_bm.o,
                          enum_status=bm.inner_bm.enum_status,
                          sd=bm.inner_bm.sd,
                          fsc=bm.inner_bm.fsc,
                          )

if __name__ == "__main__":
    from flytekit.clis.sdk_in_container import pyflyte
    from click.testing import CliRunner
    import os

    runner = CliRunner()
    path = os.path.realpath(__file__)

    input_val = BM().model_dump_json()

    print(input_val)
    result = runner.invoke(pyflyte.main,
                           ["run", path, "wf", "--bm", input_val])
    print("Local Execution: ", result.output)
    # # #
    result = runner.invoke(pyflyte.main,
                           ["run", "--remote", path, "wf", "--bm", input_val])
    print("Remote Execution: ", result.output)
