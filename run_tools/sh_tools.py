import subprocess
import sys

def sh_call(cmd, shell=False, catch_stdout=False, decode=True, expected_return_codes = [ 0 ], verbose=0):
    cmd_str = []
    for s in cmd:
        if ' ' in s:
            s = f"'{s}'"
        cmd_str.append(s)
    cmd_str = ' '.join(cmd_str)
    if verbose > 0:
        print(f'>> {cmd_str}', file=sys.stderr)
    kwargs = {
        'shell': shell,
    }
    if catch_stdout:
        kwargs['stdout'] = subprocess.PIPE
    proc = subprocess.Popen(cmd, **kwargs)
    output, err = proc.communicate()
    if proc.returncode not in expected_return_codes:
        raise RuntimeError(f'Error while running "{cmd_str}". Error code: {proc.returncode}')
    if catch_stdout and decode:
        output = [ s for s in output.decode("utf-8").split('\n') if len(s) > 0 ]
    return proc.returncode, output
