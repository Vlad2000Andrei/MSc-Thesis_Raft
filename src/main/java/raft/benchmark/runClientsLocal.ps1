Param(
    [Parameter(Mandatory=$true)]
    [string]$filePath,

    [Parameter(Mandatory=$false)]
    [string]$jarPath = '"C:\Users\cursa\Documents\University\MSc` CS\Thesis\MSc_Thesis_Raft\target\MSc_Thesis_Raft-0.1-jar-with-dependencies.jar"'
)

# Check if a file path was provided
if (!$filePath) {
    Write-Error "Please provide a file path as an argument."
    exit 1
}

# Read the lines of the file
$lines = Get-Content $filePath

# Loop through each line and open a new PowerShell window
$processIds = @()
foreach ($line in $lines) {
    $values = $line.Split(' ')
    $thisId = $values[0]
    $process = Start-Process powershell.exe -PassThru -ArgumentList "-NoExit java -jar $jarPath $thisID $filePath"
    $processIds += $process
}

Write-Host "Started processes:"
$processIds
