@echo off
if exist nupkg rd /s /q nupkg
mkdir nupkg
cd nupkg
set nuget="..\tool\nuget.exe"
%nuget% pack ..\nuget.nuspec
pause
