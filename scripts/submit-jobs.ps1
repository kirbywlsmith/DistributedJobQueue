# Submits a mix of jobs to the API, then polls their status.
# Usage:  .\scripts\submit-jobs.ps1            (one of each type)
#         .\scripts\submit-jobs.ps1 -Cpu 10    (plus a burst of 10 cpu jobs)

param(
    [string]$ApiUrl = "http://localhost:8080",
    [int]$Cpu = 0
)

$ids = @()

function Submit($type, $payload) {
    $body = @{ type = $type; payload = $payload } | ConvertTo-Json -Compress
    $resp = Invoke-RestMethod -Method Post -Uri "$ApiUrl/jobs" -ContentType 'application/json' -Body $body
    Write-Host ("submitted {0,-6} {1}" -f $type, $resp.id)
    return $resp.id
}

# one of each handler type
$ids += Submit "sleep" @{ seconds = 3 }
$ids += Submit "cpu"   @{ iterations = 3000000 }
$ids += Submit "flaky" @{ failure_rate = 0.7 }   # will likely retry a few times

# optional burst of cpu jobs (for the autoscaling demo)
for ($i = 0; $i -lt $Cpu; $i++) {
    $ids += Submit "cpu" @{ iterations = 5000000 }
}

Write-Host "`nwaiting for jobs to finish...`n"
Start-Sleep -Seconds 8

foreach ($id in $ids) {
    $j = Invoke-RestMethod -Uri "$ApiUrl/jobs/$id"
    Write-Host ("{0}  {1,-9} attempts={2}" -f $j.id, $j.status, $j.attempts)
}
