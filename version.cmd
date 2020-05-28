@echo off
set v=1.1.0
tool\EditVersion dir="%cd%" v=%v% a="Assembly.cs"