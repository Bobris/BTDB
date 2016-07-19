$assemblyInfoPath = "Properties\AssemblyInfo.cs"
$nuspecPath = "BTDB.Sources.nuspec"

#parse version from assemblyinfo
$regex = '^\[assembly: AssemblyVersion\("(.*?)"\)\]'
$assemblyInfo = Get-Content $assemblyInfoPath -Raw

$version = [Regex]::Match(
        $assemblyInfo, 
        $regex,
        [System.Text.RegularExpressions.RegexOptions]::Multiline
    ).Groups[1].Value

#update version in nuspec file

$nuspec = [xml](gc $nuspecPath)
$ns = New-Object Xml.XmlNamespaceManager $nuspec.NameTable
$ns.AddNamespace("nsns", "http://schemas.microsoft.com/packaging/2010/07/nuspec.xsd")

$versionNode = $nuspec.SelectSingleNode('//nsns:version', $ns)

$versionNode."#text" = $version
$nuspec.Save($nuspecPath)

Write-Output "Version in $nuspecPath updated to $version"