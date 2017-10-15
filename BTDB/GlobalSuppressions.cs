
// This file is used by Code Analysis to maintain SuppressMessage 
// attributes that are applied to this project.
// Project-level suppressions either have no target or are given 
// a specific target and scoped to a namespace, type, member, etc.

[assembly: System.Diagnostics.CodeAnalysis.SuppressMessage("Potential Code Quality Issues", "RECS0022:A catch clause that catches System.Exception and has an empty body", Justification = "Not problem", Scope = "module")]
[assembly: System.Diagnostics.CodeAnalysis.SuppressMessage("Language Usage Opportunities", "RECS0011:Convert 'if' to '?:'", Justification = "It would be harder to read", Scope = "module")]
[assembly: System.Diagnostics.CodeAnalysis.SuppressMessage("Potential Code Quality Issues", "RECS0021:Warns about calls to virtual member functions occuring in the constructor", Justification = "Not problem", Scope = "member", Target = "~M:BTDB.StreamLayer.PositionLessStreamReader.#ctor(BTDB.StreamLayer.IPositionLessStream)")]
[assembly: System.Diagnostics.CodeAnalysis.SuppressMessage("Language Usage Opportunities", "RECS0091:Use 'var' keyword when possible", Justification = "Bug in analyzer", Scope = "member", Target = "~M:BTDB.IOC.SingleFactoryRegistration.Register(BTDB.IOC.ContanerRegistrationContext)")]
[assembly: System.Diagnostics.CodeAnalysis.SuppressMessage("Readability", "RCS1018:Add default access modifier.", Justification = "It would be harder to read")]

