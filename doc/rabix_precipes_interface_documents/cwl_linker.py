import json, yaml, sys


def do_import(obj):
    if isinstance(obj, list):   
      return [do_import(x) for x in obj]
    if not isinstance(obj, dict):
        return obj
    if len(obj) == 1 and obj.get('import'):
        with open(obj['import']) as fp: return yaml.load(fp)
    return {k: do_import(v) for k, v in obj.iteritems()}
    
    
if __name__ == '__main__':
  with open(sys.argv[1]) as fp:
    wf = yaml.load(fp)
  print json.dumps(do_import(wf), indent=2)
  
  