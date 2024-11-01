import typing
import os
from dataclasses import dataclass, field
from typing import Dict, List
from flytekit.types.file import FlyteFile
from flytekit.types.directory import FlyteDirectory
from flytekit.types.structured import StructuredDataset
from flytekit.types.schema import FlyteSchema
from flytekit import task, workflow, ImageSpec
from enum import Enum
import pandas as pd

image = "futureoutlier/flytekit:6oCREy62XKWzgJswOZyZUg"

class Status(Enum):
    PENDING = "pending"
    APPROVED = "approved"
    REJECTED = "rejected"

@dataclass
class InnerDC:
    a: int = -1
    b: float = 2.1
    c: str = "Hello, Flyte"
    d: bool = False
    e: List[int] = field(default_factory=lambda: [0, 1, 2, -1, -2])
    f: List[FlyteFile] = field(default_factory=lambda: [FlyteFile("s3://my-s3-bucket/s3_flyte_dir/example.txt")])
    g: List[List[int]] = field(default_factory=lambda: [[0], [1], [-1]])
    h: List[Dict[int, bool]] = field(default_factory=lambda: [{0: False}, {1: True}, {-1: True}])
    i: Dict[int, bool] = field(default_factory=lambda: {0: False, 1: True, -1: False})
    j: Dict[int, FlyteFile] = field(default_factory=lambda: {
        0: FlyteFile("s3://my-s3-bucket/s3_flyte_dir/example.txt"),
        1: FlyteFile("s3://my-s3-bucket/s3_flyte_dir/example.txt"),
        -1: FlyteFile("s3://my-s3-bucket/s3_flyte_dir/example.txt")
    })
    k: Dict[int, List[int]] = field(default_factory=lambda: {0: [0, 1, -1]})
    l: Dict[int, Dict[int, int]] = field(default_factory=lambda: {1: {-1: 0}})
    m: dict = field(default_factory=lambda: {"key": "value"})
    n: FlyteFile = field(default_factory=lambda: FlyteFile("s3://my-s3-bucket/s3_flyte_dir/example.txt"))
    o: FlyteDirectory = field(default_factory=lambda: FlyteDirectory("s3://my-s3-bucket/s3_flyte_dir"))
    enum_status: Status = field(default=Status.PENDING)
    sd: StructuredDataset = field(default_factory=lambda: StructuredDataset(uri="s3://my-s3-bucket/s3_flyte_dir/df.parquet", file_format="parquet"))
    fsc: FlyteSchema = field(default_factory=lambda: FlyteSchema(remote_path="s3://my-s3-bucket/s3_flyte_dir/df.parquet"))

@dataclass
class DC:
    a: int = -1
    b: float = 2.1
    c: str = "Hello, Flyte"
    d: bool = False
    e: List[int] = field(default_factory=lambda: [0, 1, 2, -1, -2])
    f: List[FlyteFile] = field(default_factory=lambda: [FlyteFile("s3://my-s3-bucket/s3_flyte_dir/example.txt")])
    g: List[List[int]] = field(default_factory=lambda: [[0], [1], [-1]])
    h: List[Dict[int, bool]] = field(default_factory=lambda: [{0: False}, {1: True}, {-1: True}])
    i: Dict[int, bool] = field(default_factory=lambda: {0: False, 1: True, -1: False})
    j: Dict[int, FlyteFile] = field(default_factory=lambda: {
        0: FlyteFile("s3://my-s3-bucket/s3_flyte_dir/example.txt"),
        1: FlyteFile("s3://my-s3-bucket/s3_flyte_dir/example.txt"),
        -1: FlyteFile("s3://my-s3-bucket/s3_flyte_dir/example.txt")
    })
    k: Dict[int, List[int]] = field(default_factory=lambda: {0: [0, 1, -1]})
    l: Dict[int, Dict[int, int]] = field(default_factory=lambda: {1: {-1: 0}})
    m: dict = field(default_factory=lambda: {"key": "value"})
    n: FlyteFile = field(default_factory=lambda: FlyteFile("s3://my-s3-bucket/s3_flyte_dir/example.txt"))
    o: FlyteDirectory = field(default_factory=lambda: FlyteDirectory("s3://my-s3-bucket/s3_flyte_dir"))
    inner_dc: InnerDC = field(default_factory=lambda: InnerDC())
    enum_status: Status = field(default=Status.PENDING)
    sd: StructuredDataset = field(default_factory=lambda: StructuredDataset(uri="s3://my-s3-bucket/s3_flyte_dir/df.parquet", file_format="parquet"))
    fsc: FlyteSchema = field(default_factory=lambda: FlyteSchema(remote_path="s3://my-s3-bucket/s3_flyte_dir/df.parquet"))

@task(container_image=image)
def t_dc(dc: DC) -> DC:
    return dc

@task(container_image=image)
def t_inner(inner_dc: InnerDC):
    assert isinstance(inner_dc, InnerDC), "inner_dc is not of type InnerDC"

    expected_file_content = "Default content"

    # f: List[FlyteFile]
    for ff in inner_dc.f:
        assert isinstance(ff, FlyteFile), "Expected FlyteFile"
        with open(ff, "r") as f:
            assert f.read() == expected_file_content, "File content mismatch in f"

    # j: Dict[int, FlyteFile]
    for _, ff in inner_dc.j.items():
        assert isinstance(ff, FlyteFile), "Expected FlyteFile in j"
        with open(ff, "r") as f:
            assert f.read() == expected_file_content, "File content mismatch in j"

    # n: FlyteFile
    assert isinstance(inner_dc.n, FlyteFile), "n is not FlyteFile"
    with open(inner_dc.n, "r") as f:
        assert f.read() == expected_file_content, "File content mismatch in n"

    # o: FlyteDirectory
    assert isinstance(inner_dc.o, FlyteDirectory), "o is not FlyteDirectory"
    assert not inner_dc.o.downloaded, "o should not be downloaded initially"
    with open(os.path.join(inner_dc.o, "example.txt"), "r") as fh:
        assert fh.read() == expected_file_content, "File content mismatch in o"
    assert inner_dc.o.downloaded, "o should be marked as downloaded after access"

    assert inner_dc.enum_status == Status.PENDING, "enum_status does not match"
    assert isinstance(inner_dc.sd, StructuredDataset), "sd is not StructuredDataset"
    assert isinstance(inner_dc.fsc, FlyteSchema), "fsc is not FlyteSchema"
    print("All checks in InnerDC passed")

@task(container_image=image)
def t_test_all_attributes(a: int, b: float, c: str, d: bool, e: List[int], f: List[FlyteFile], g: List[List[int]],
                          h: List[Dict[int, bool]], i: Dict[int, bool], j: Dict[int, FlyteFile],
                          k: Dict[int, List[int]], l: Dict[int, Dict[int, int]], m: dict,
                          n: FlyteFile, o: FlyteDirectory, enum_status: Status, sd: StructuredDataset, fsc: FlyteSchema):
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
def wf(dc: DC):
    t_dc(dc=dc)
    t_inner(dc.inner_dc)
    t_test_all_attributes(a=dc.a, b=dc.b, c=dc.c,
                          d=dc.d, e=dc.e, f=dc.f,
                          g=dc.g, h=dc.h, i=dc.i,
                          j=dc.j, k=dc.k, l=dc.l,
                          m=dc.m, n=dc.n, o=dc.o,
                          enum_status=dc.enum_status,
                          sd=dc.sd, fsc=dc.fsc)
    print("Workflow execution completed")

if __name__ == "__main__":
    from flytekit.clis.sdk_in_container import pyflyte
    from click.testing import CliRunner

    runner = CliRunner()
    path = os.path.realpath(__file__)
    input_val = '{"a": -1, "b": 3.14}'
    result = runner.invoke(pyflyte.main, ["run", path, "wf", "--dc", input_val])
    print("Local Execution: ", result.output)
    result = runner.invoke(pyflyte.main, ["run", "--remote", path, "wf", "--dc", input_val])
    print("Remote Execution: ", result.output)
