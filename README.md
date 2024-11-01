# demo-msgpack
These examples show how msgpack idl works in dataclass, pydantic, map task, and raw container task(copilot) in Flyte.

To run these examples, you need to do the following steps:

1. start your sandbox by this image:
```zsh
flytectl demo start --image futureoutlier/flyte-sandbox:msgpack-idl-1101 --force
```
2. upload the `s3_flyte_dir` to your s3 bucket `s3://my-s3-bucket`:
3. use this image to run every task
```zsh
futureoutlier/flytekit:6oCREy62XKWzgJswOZyZUg
```
