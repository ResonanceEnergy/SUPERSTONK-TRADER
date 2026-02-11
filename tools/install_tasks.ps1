param(
  [string]$ProjectPath = (Resolve-Path "..").Path
)

$Python = Join-Path $ProjectPath ".venv\Scripts\python.exe"
$Script = Join-Path $ProjectPath "dd_library_autopilot.py"

if (!(Test-Path $Python)) {
  Write-Host "Python venv not found at $Python. Create venv first." -ForegroundColor Red
  exit 1
}

$Action = New-ScheduledTaskAction -Execute $Python -Argument "`\"$Script`\" --max-minutes 20" -WorkingDirectory $ProjectPath
$Trigger = New-ScheduledTaskTrigger -Once -At (Get-Date).AddMinutes(1) -RepetitionInterval (New-TimeSpan -Hours 6) -RepetitionDuration ([TimeSpan]::MaxValue)

$Settings = New-ScheduledTaskSettingsSet `
  -AllowStartIfOnBatteries `
  -DontStopIfGoingOnBatteries `
  -StartWhenAvailable `
  -MultipleInstances IgnoreNew `
  -ExecutionTimeLimit (New-TimeSpan -Minutes 25)

Register-ScheduledTask -TaskName "Superstonk DD Autopilot (6h-20m)" -Action $Action -Trigger $Trigger -Settings $Settings -RunLevel Highest -Force
Write-Host "Installed: Superstonk DD Autopilot (6h-20m)" -ForegroundColor Green

$Action2 = New-ScheduledTaskAction -Execute $Python -Argument "`\"$Script`\" --recrawl-hubs --max-minutes 20" -WorkingDirectory $ProjectPath
$Trigger2 = New-ScheduledTaskTrigger -Monthly -DaysOfMonth 1 -At 3:00AM
Register-ScheduledTask -TaskName "Superstonk DD Hub Refresh (Monthly-20m)" -Action $Action2 -Trigger $Trigger2 -Settings $Settings -RunLevel Highest -Force
Write-Host "Installed: Superstonk DD Hub Refresh (Monthly-20m)" -ForegroundColor Green
