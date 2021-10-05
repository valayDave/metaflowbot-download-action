import traceback
from urllib.parse import urlparse
from .parser import LanguageParser
import click
import timeago
from metaflowbot.exceptions import MFBException

from metaflowbot.cli import action
from metaflowbot.message_templates.templates import error_message,HEADINGS
from metaflowbot.state import MFBState
from metaflow import Flow, Run,namespace
from metaflow.exception import MetaflowNotFound
from metaflow.datatools.s3 import MetaflowS3AccessDenied

MAX_ARTIFACT_SIZE = 1000
import json

import requests


PARSE = LanguageParser([
    "download <latest> <artifactname> from <flow>",
    "download <artifactname> from <flow>/<runid>",
])

class ArtifactNotFound(MFBException):
    def __init__(self,pathspec,artname):
        super().__init__(f"Cannot Resolve Artifact named {artname} from {pathspec}")

class InvalidPath(MFBException):
    def __init__(self,pathspec,artname):
        super().__init__(f"Path in the artifact named {artname} from {pathspec} is not a S3 path. S3 Path required")

def _to_file(file_bytes):
    """
    Returns temporary files for a blob of bytes. 
    """
    import tempfile
    latent_temp = tempfile.NamedTemporaryFile()
    latent_temp.write(file_bytes)
    latent_temp.seek(0)
    return latent_temp

def resolve_artifact(run_obj:Run,info_obj):
    # find out if artifact is an s3 url ; 
    artname = info_obj['artifactname']
    try:
        artifact = run_obj.data._artifacts[artname]
    except KeyError:
        raise ArtifactNotFound(run_obj.pathspec,artname)
    
    artval = artifact.data
    if type(artval) is str:
        if 's3://' in artval:
            # Returns valid S3 url
            return artval
    raise InvalidPath


def heartbeat(func):
    
    def wrapper(context,*args,**kwargs):
        context


        
    
def _upload_to_slack(clickcontext,tempfile,artname,s3path,):
    filename = s3path.split('/')[-1]
    channel, thread_ts = clickcontext.obj.thread.split(":")
    clickcontext.obj.reply(f"Downloaded Artifact, Uploading to Slack {filename}")
    clickcontext.obj.sc.sc.files_upload(file=tempfile,\
                            filename=filename,\
                            channels=channel,\
                            thread_ts=thread_ts,\
                            title = artname)

def _check_and_populate(data,keys):
    obj = {key:None for key in keys }
    for key in keys:
        if key in data:
            obj[key] = data[key]
    return obj

def resolve_message(message):
    # Return a run Object
    message_dict = PARSE(message)
    info_dict = _check_and_populate(message_dict,['flow', 'runid', 'latest', 'artifactname'])
    
    if info_dict['flow'] is None or info_dict['artifactname'] is None:
        return None,None
    elif info_dict['runid'] is None and info_dict['latest'] is None:
        return None,None
    run = None
    if info_dict['latest']:
        run = Flow(info_dict['flow']).latest_successful_run
        
    else:
        runidstr = f"{info_dict['flow']}/{info_dict['runid']}"
        try:
            run = Run(runidstr)
        except MetaflowNotFound as e: 
            run = None
    
    return run,info_dict
    


def create_howto_message():
    return ''.join([
        "You can download an artifact stored on S3 using the `donwnload` command \n",
        "To download artifact from latest run of a flow : `download latest <artifactname> from HelloFlow`\n",
        "To download artifact from a specific run : `download <artifactname> from HelloFlow/12`\n\n",
        "_<artifactname> can be any properties set in the `end` step_"
    ])



@action.command(help="How to download")
@click.option("--create-thread/--no-create-thread", help="Will create a new thread")
@click.pass_context
def how_to_download(ctx, create_thread=False):
    obj = ctx.obj
    if create_thread:
        obj.publish_state(MFBState.message_new_thread(obj.thread))
    try:
        message = create_howto_message()
        obj.reply(
            message
        )
    except:
        traceback.print_exc()
        my_traceback = traceback.format_exc()
        err_msg = "Sorry, I couldn't find a joke at the moment :meow_dead:"
        obj.reply(err_msg, **error_message(my_traceback, message=err_msg))




@action.command(help="download ")
@click.option("--message", help="Actual message")
@click.option("--create-thread/--no-create-thread", help="Will create a new thread")
@click.pass_context
def download(ctx, message=None,create_thread=False):
    namespace(None)
    from metaflow import S3
    obj = ctx.obj
    if create_thread:
        obj.publish_state(MFBState.message_new_thread(obj.thread))
    try:
        run_obj,message_info =  resolve_message(message)
        if run_obj is None:
            obj.reply(HEADINGS.NO_RUNS)
            return 
        try:
            s3_path = resolve_artifact(run_obj,message_info)
            obj.reply("Ok, found the S3 URL of the artifact. Downloading now. ")
        except InvalidPath as e:
            obj.reply(e.msg)
            return 
        except ArtifactNotFound as e:
            obj.reply(e.msg)
            return             
        try:
            with S3() as s3:
                s3_resp = s3.get(s3_path)
                _upload_to_slack(ctx,s3_resp.blob,message_info['artifactname'],s3_path,)
                
        except MetaflowS3AccessDenied as e:
            obj.reply(f"Unable to download the Object from S3 `{s3_path}` :meow_dead:")
            return
            
    except:
        traceback.print_exc()
        my_traceback = traceback.format_exc()
        err_msg = "Sorry, I couldn't find you were looking for :meow_dead:"
        obj.reply(err_msg, **error_message(my_traceback, message=err_msg))
