import importlib
import inspect
import json
import sys

from mpi4py import MPI


def main():
    module_path, func_name = sys.argv[1], sys.argv[2]
    kwargs = json.loads(sys.argv[3]) if sys.argv[3] != "null" else {}

    func = getattr(importlib.import_module(module_path), func_name)
    comm = MPI.COMM_WORLD

    if "comm" in inspect.signature(func).parameters:
        kwargs["comm"] = comm

    result = func(**kwargs)

    comm.Barrier()

    if comm.Get_rank() == 0:
        print(f"MPIRESULT:{json.dumps(result)}", flush=True)


if __name__ == "__main__":
    main()
