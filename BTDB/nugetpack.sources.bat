rmdir /s /q tmpDist
if not exist tmpDist goto copyFiles
echo Cannot delete tmpDist
exit /b 1

:copyFiles
robocopy . tmpDist/BTDB *.* /S /XD obj /XD bin /XD tmpDist
del tmpDist\BTDB\*.* /q
rmdir /s /q tmpDist\BTDB\Properties
copy GlobalSuppressions.cs tmpDist\BTDB

PowerShell -ExecutionPolicy Unrestricted -noprofile -file nugetpack.sources.version.ps1

nuget pack BTDB.Sources.nuspec -BasePath tmpDist

rmdir /s /q tmpDist