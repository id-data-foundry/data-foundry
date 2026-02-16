#!/usr/bin/env python3

from vosk import Model, KaldiRecognizer, SetLogLevel
import sys
import os
import wave
import json

SetLogLevel(-1)

if not os.path.exists("/home/data/foundry/processing/stt/model"):
    print ("Please download the model from https://alphacephei.com/vosk/models and unpack as 'model' in the current folder.")
    exit (1)

wf = wave.open(sys.argv[1], "rb")
if wf.getnchannels() != 1 or wf.getsampwidth() != 2 or wf.getcomptype() != "NONE":
    print ("Audio file must be WAV format mono PCM.")
    exit (1)

model = Model("/home/data/foundry/processing/stt/model")
rec = KaldiRecognizer(model, wf.getframerate())

while True:
    data = wf.readframes(4000)
    if len(data) == 0:
        break
    if rec.AcceptWaveform(data):
        res = json.loads(rec.Result())
        print(res['text'])
    else:
        #print(rec.PartialResult())
        pass

#print(rec.FinalResult())
