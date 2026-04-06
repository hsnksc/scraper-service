$ErrorActionPreference = "Stop"
Set-Location "E:\searchforemlak"

$env:PYTHONPATH = "E:\searchforemlak\.vendor;E:\searchforemlak"
$env:HOST = "127.0.0.1"
$env:PORT = "8010"
py -3 .\main.py
