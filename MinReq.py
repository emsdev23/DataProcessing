import requests
import time


headers = {'Authorization': 'VKOnNhH2SebMU6S'}

dgminurl = 'http://localhost:8004/dgMinwise'
whgdminurl = 'http://localhost:8004/WhGdMinwise'
wdminurl = 'http://localhost:8004/WdMinwise'

def dgmin():
    print("DG Minute")
    upsres = requests.get(dgminurl, headers=headers)
    print(upsres.json())
    time.sleep(5)

def whgdmin():
    print("WH GD Minute")
    upsres = requests.get(whgdminurl, headers=headers)
    print(upsres.json())
    time.sleep(5)

def wdmin():
    print("WD Minute")
    upsres = requests.get(wdminurl, headers=headers)
    print(upsres.json())
    time.sleep(5)

function_names = ['wdmin']

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