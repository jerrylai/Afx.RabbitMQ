@echo off
set Build="%SYSTEMDRIVE%\Program Files\Microsoft Visual Studio\2022\Enterprise\MSBuild\Current\Bin\MsBuild.exe"
if exist publish rd /s /q publish
dotnet build "NET6.0/Afx.RabbitMQ/Afx.RabbitMQ.csproj" -c Release
dotnet build "NET8.0/Afx.RabbitMQ/Afx.RabbitMQ.csproj" -c Release
cd publish
del /q/s *.pdb
del /q/s Newtonsoft*
del /q/s RabbitMQ.Client*
del /q/s System*
del /q/s Microsoft*
pause