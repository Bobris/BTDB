﻿//HintName: IItemTable.g.cs
// <auto-generated/>
#pragma warning disable 612,618
#nullable enable
using System;
using System.Runtime.CompilerServices;
// Name: IItemTable
// Field: CompanyId ulong
//           PrimaryIndex: 1
// Field: Queue string reference
//           PrimaryIndex: 2
// Field: ItemId Guid reference
//           PrimaryIndex: 3
// Field: Priority int
//           SecondaryIndex LockDeadline: 3 IncludePrimaryKeyOrder 2
// Field: LockDeadline DateTime reference
//           SecondaryIndex LockDeadline: 4
[CompilerGenerated]
static file class IItemTableRegistration
{
    [ModuleInitializer]
    internal static unsafe void Register4BTDB()
    {
    }
}
