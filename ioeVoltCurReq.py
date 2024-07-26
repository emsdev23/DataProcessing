import requests
import time


headers = {'Authorization': 'VKOnNhH2SebMU6S'}

st1url = 'http://localhost:8005/st1CurVol'
st2url = 'http://localhost:8005/st2CurVol'
st3url = 'http://localhost:8005/st3CurVol'
st4url = 'http://localhost:8005/st4CurVol'
st5url = 'http://localhost:8005/st5CurVol'
sumurl = 'http://localhost:8005/sumCurVol'

def str1fun():
    print("ST1 cur vs volt")
    upsres = requests.get(st1url, headers=headers)
    print(upsres.json())
    time.sleep(10)

def str2fun():
    print("ST2 cur vs volt")
    upsres = requests.get(st2url, headers=headers)
    print(upsres.json())
    time.sleep(10)

def str3fun():
    print("ST3 cur vs volt")
    upsres = requests.get(st3url, headers=headers)
    print(upsres.json())
    time.sleep(10)

def str4fun():
    print("ST4 cur vs volt")
    upsres = requests.get(st4url, headers=headers)
    print(upsres.json())
    time.sleep(10)

def str5fun():
    print("ST5 cur vs volt")
    upsres = requests.get(st5url, headers=headers)
    print(upsres.json())
    time.sleep(10)

def sumfun():
    print("SUM cur vs volt")
    upsres = requests.get(sumurl, headers=headers)
    print(upsres.json())
    time.sleep(10)

function_names = ['sumfun','str1fun','str2fun','str3fun','str4fun','str5fun']

while True:
    for name in function_names:
        if name in globals() and callable(globals()[name]):
            function = globals()[name]
            try:
                result = function()
            except:
                continue
        else:
            print(f"Function {name} not found.")    
   
    time.sleep(120)