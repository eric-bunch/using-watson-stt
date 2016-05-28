"""
This assumes that we are working in an environment running Python 2.7.11, where the result of pip freeze is exactly

attrs==15.2.0
autobahn==0.13.1
boto==2.39.0
cffi==1.5.2
cryptography==1.3.1
enum34==1.1.2
idna==2.1
ipaddress==1.0.16
pyasn1==0.1.9
pyasn1-modules==0.0.8
pycparser==2.14
pyOpenSSL==16.0.0
requests==2.9.1
service-identity==16.0.0
six==1.10.0
Twisted==16.1.1
txaio==2.3.1
zope.interface==4.1.3

The assumed file structure before running the script is
.
+--audio_wfc_tmp
+--_output
|  +-- hypotheses.txt
+--_recordings
+--transcribe_s3.py
+--README.md
+--recordings.txt
+--requirements.txt
+--sttClient.py
"""


import boto
from boto.s3.connection import S3Connection
from boto.s3.key import Key
from subprocess import call
import os
import timeit


# credentials
## S3 credentials
s3 = S3Connection('','')
## ibm credentials
ibmuser = ""
ibmpass = ""

bucket = s3.get_bucket('bucket_name')



def from_s3_chunked(chunk, local_path):
	if os.path.isdir(local_path + "audio_tmp"):
		call(["rm", "-r", local_path + "audio_tmp"])

	call(["mkdir", local_path + "audio_tmp"])

	recs = open(local_path + "recordings.txt", "w")

	for key in chunk:
		key.get_contents_to_filename(local_path + "audio_tmp/" + str(key.name).replace("AUDIO_FILES/", ""))
		name = str(key.name).replace("AUDIO_FILES/", "")
		name = name.split(".")[0] + "_pcm.wav"
		recs.write("./recordings/" + name)
		if chunk[-1] != key:
			recs.write("\n")





def convert_audiofiles_chunked(local_path):
	
	if os.path.isdir(local_path + "recordings"):
		call(["rm", "-r", local_path + "recordings"])

	call(["mkdir", local_path + "recordings"])


	for f in os.listdir(local_path + "audio_tmp"):
		fname = f.split(".")[0]
		call(["ffmpeg", "-i", local_path + "audio_tmp/" + f, 
                  "-acodec", 
                  "pcm_s16le", "-ac", "1", 
                  local_path + "recordings/" + fname + "_pcm.wav"]) 



def transcribe_chunks(local_path):
	if os.path.isdir(local_path + "output"):
		call(["rm", "-r", local_path + "output"])

	num_threads = str(len([x for x in os.listdir(local_path + "recordings") if ".wav" in x]))

	call(["python", local_path + "sttClient.py", "-credentials", 
         ibmuser + ":" + ibmpass, "-model", "en-US_NarrowbandModel", "-threads", num_threads])



def to_s3_chunked(local_path):
	hyps = open(local_path + "output/hypotheses.txt").read()
	hyps = hyps.split("\n")

	k = Key(bucket)

	for index, f in enumerate([x for x in os.listdir(local_path + "output") if "json" in x]):

		audio_file = os.listdir(local_path + "audio_tmp")[index]
		fname = audio_file.split(".")[0] # filename w/o extension

		k.key = "AUDIO_FILES/transcribed/" + fname + "/tscp_json.txt"
		k.set_contents_from_filename(local_path + "output/" + f)

		k.key = "AUDIO_FILES/transcribed/" + fname + "/tscp.txt"
		if index in range(0, len(hyps)):
			k.set_contents_from_string(hyps[index])



		


# main script
def main_chunked(chunkby=1):
	tic = timeit.default_timer()

	LOCALPATH = os.path.dirname(os.path.abspath(__file__)) + "/"

	count = 0
	srm_bucket_list = [x for x in bucket.list(prefix='AUDIO_FILES/', delimiter='/') if ".wav" in x.name]
	N = len(srm_bucket_list)
	l = range(0, N)
	chunks = l[::chunkby]

	for i in chunks:
		if chunks[-1] != i:
			chunk = srm_bucket_list[i:i + chunkby]
		else:
			chunk = srm_bucket_list[i:N]
 

		from_s3_chunked(chunk, LOCALPATH)

		convert_audiofiles_chunked(LOCALPATH)

		transcribe_chunks(LOCALPATH)

		to_s3_chunked(LOCALPATH)
		count += len(chunk)


	toc = timeit.default_timer()
	t = toc - tic

	k = Key(bucket)
	time_output_string = str(t) + " (time elapsed in seconds) " + count + " (number audio files)"
	k.key = "SRM_AUDIO_FILES/transcribed/time_output.txt"
	k.set_contents_from_string(time_output_string)




if __name__ == '__main__':
	main_chunked(100)
