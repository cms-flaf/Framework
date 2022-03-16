import law
import luigi
import os
import re

from run_tools.sh_tools import sh_call

class CreateVomsProxy(law.Task):
    time_limit = luigi.Parameter(default=24)

    def __init__(self, *args, **kwargs):
        super(CreateVomsProxy, self).__init__(*args, **kwargs)
        self.proxy_path = os.getenv("X509_USER_PROXY")
        if os.path.exists(self.proxy_path):
            proxy_info = self.get_proxy_info()
            timeleft = self.get_proxy_timeleft(proxy_info)
            if timeleft < self.time_limit:
                print(f"Removing old proxy which expires in a less than {timeleft:.1f} hours.")
                proxy_file.remove()

    def output(self):
        return law.LocalFileTarget(self.proxy_path)

    def create_proxy(self, proxy_file):
        print("Creating voms proxy...")
        proxy_file.makedirs()
        sh_call(['voms-proxy-init', '-voms', 'cms', '-rfc', '-valid', '192:00', '--out', proxy_file.path])

    def get_proxy_info(self):
        _, output = sh_call(['voms-proxy-info'], catch_stdout=True)
        info = {}
        for line in output:
            print(line)
            match = re.match(r'^(.+) : (.+)', line)
            key = match.group(1).strip()
            info[key] = match.group(2)
        return info

    def get_proxy_timeleft(self, proxy_info):
        h,m,s = proxy_info['timeleft'].split(':')
        return float(h) + ( float(m) + float(s) / 60. ) / 60.

    def run(self):
        proxy_file = self.output()
        self.create_proxy(proxy_file)
        if not proxy_file.exists():
            raise RuntimeError("Unable to create voms proxy")
