"""Generate/check synthetic ST04 assets; does not enable runtime support."""
import argparse
from pathlib import Path
from poststart_profile.tooling import check, generate

def main():
    root=Path(__file__).resolve().parent.parent
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--check',action='store_true')
    parser.add_argument('--output-dir',type=Path,default=root/'contracts/candidates/st04-poststart-v1')
    args=parser.parse_args()
    specs=[root/'docs/decisions'/name for name in ('ST04-poststart.md','ST04-poststart-schema.md')]
    (check if args.check else generate)(args.output_dir,specs)
if __name__=='__main__': main()
