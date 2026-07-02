import base64
import inspect
import json
import sys

import cloudpickle
from mpi4py import MPI


def main():
    func_string = sys.argv[1]
    kwargs = json.loads(sys.argv[2]) if sys.argv[2] != "null" else {}

    func = cloudpickle.loads(base64.b64decode(func_string.encode("ascii")))
    comm = MPI.COMM_WORLD

    if "comm" in inspect.signature(func).parameters:
        kwargs["comm"] = comm

    result = func(**kwargs)

    comm.Barrier()

    if comm.Get_rank() == 0:
        print(f"MPIRESULT:{json.dumps(result)}", flush=True)


if __name__ == "__main__":
    main()
