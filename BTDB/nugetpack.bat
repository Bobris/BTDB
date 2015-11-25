nuget pack -Build -Symbols -Properties Configuration=Release BTDB.csproj
nuget push *.nupkg
