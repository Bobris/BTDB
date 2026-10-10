# Build context: app/ (dotnet publish -r linux-x64 output) and ca.crt (the CA that signed the peer certificate).
FROM mcr.microsoft.com/dotnet/aspnet:10.0
COPY ca.crt /usr/local/share/ca-certificates/eventlog-bench-ca.crt
RUN update-ca-certificates
WORKDIR /app
COPY app/ .
ENTRYPOINT ["dotnet", "EventLogBench.dll"]
