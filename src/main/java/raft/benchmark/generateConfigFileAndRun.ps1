Param(
    [Parameter(Mandatory=$true)]
    [string]$address,

    [Parameter(Mandatory=$true)]
    [Int32]$startPort,

    [Parameter(Mandatory=$true)]
    [Int32]$count,

    [Parameter(Mandatory=$true)]
    [string]$jarPath,

    [Parameter(Mandatory=$true)]
    [string]$controllerAddr,

    [Parameter(Mandatory=$true)]
    [string]$controllerPort
)

# Get the full path to the script file
$scriptPath = $MyInvocation.MyCommand.Path

# Get the directory where the script is stored
$scriptDirectory = Split-Path $scriptPath

# Create the file name
$fileName = "$count$address$startPort.raftcfg"

# Create the full path to the file
$filePath = Join-Path $scriptDirectory $fileName

# Check if the file already exists and delete it if necessary
if (Test-Path $filePath) {
    Remove-Item -Path $filePath -Force
}

# Create the file
New-Item -Path $filePath -ItemType File

for ($i = 0; $i -lt $count; $i++) {
    $port = $startPort + $i
    $line = "$i $address $port"
    Add-Content -Path $filePath -Value $line
}

# Read the lines of the file
$lines = Get-Content $filePath

# Loop through each line and open a new PowerShell window
$processIds = @()
foreach ($line in $lines) {
    $values = $line.Split(' ')
    $thisId = $values[0]
    $process = Start-Process powershell.exe -PassThru -ArgumentList "-NoExit java -jar '$jarPath' worker $controllerAddr $controllerPort $thisID '$filePath'"
    $processIds += $process
}

Write-Host "Started processes $count processes."