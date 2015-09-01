import json
from flask import Flask, request
from rabix.cli.adapter import CLIJob
from rabix.common.models import Job, process_builder, construct_files
from rabix.common.context import Context
from rabix.cli import init


app = Flask(__name__)
ctx = Context(None)
init(ctx)


def strip_prefix(input_id):
    return input_id[input_id.rfind('.') + 1:]


@app.route('/get_command_line', methods=['POST'])
def get_command_line():
    data = request.get_json(force=True)
    print data
    tool = process_builder(ctx, data['tool_cfg'])
    inputs = {strip_prefix(k): construct_files(v, tool._inputs[strip_prefix(k)].validator) for k, v in data['input_map'].iteritems()}
    job = Job('Fake job ID', tool, inputs,  {'cpu': 1, 'mem': 1024}, ctx)
    cli_job = CLIJob(job)
    res = json.dumps({
        'arguments': cli_job.make_arg_list(),
        'stdin': cli_job.stdin,
        'stdout': cli_job.stdout,
    }, indent=2)
    print res
    return res


@app.route('/get_outputs', methods=['POST'])
def get_outputs():
    data = request.get_json(force=True)
    tool = process_builder(ctx, data['tool_cfg'])
    inputs = {strip_prefix(k): construct_files(v, tool._inputs[strip_prefix(k)].validator) for k, v in data['input_map'].iteritems()}
    job = Job('Fake job ID', tool, inputs,  {'cpu': 1, 'mem': 1024}, ctx)
    cli_job = CLIJob(job)
    print data
#    status = 'SUCCESS' if data['exit_code'] in data['tool_cfg'].get('successCodes', [0]) else 'FAILURE'
    status = 'SUCCESS'
    outputs = cli_job.get_outputs(data['job_dir'], job)
    if inputs:
      key = inputs.keys()[0]
      prefix = key[0:key.rfind('.')+1]
      outputs = {prefix + k: v for k, v in outputs.iteritems()}
    return json.dumps({
        'status': status,
        'outputs': outputs,
    })


if __name__ == '__main__':
    app.run(debug=True)
