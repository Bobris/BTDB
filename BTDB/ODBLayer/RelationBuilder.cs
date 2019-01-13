using System;
using System.Collections.Generic;
using System.Collections.ObjectModel;
using System.Linq;
using System.Reflection;
using BTDB.Buffer;
using BTDB.FieldHandler;
using BTDB.IL;
using BTDB.KVDBLayer;
using BTDB.StreamLayer;

namespace BTDB.ODBLayer
{
    internal class RelationBuilder<T>
    {
        readonly RelationInfo _relationInfo;

        public RelationBuilder(RelationInfo relationInfo)
        {
            _relationInfo = relationInfo;
        }

        public Func<IObjectDBTransaction, T> Build(string relationName, Type relationDBManipulatorType)
        {
            var interfaceType = typeof(T);
            var classImpl = ILBuilder.Instance.NewType("Relation" + relationName, relationDBManipulatorType, new[] { interfaceType });
            var constructorMethod = classImpl.DefineConstructor(new[] { typeof(IObjectDBTransaction), typeof(RelationInfo) });
            var il = constructorMethod.Generator;
            // super.ctor(transaction, relationInfo);
            il.Ldarg(0).Ldarg(1).Ldarg(2).Call(relationDBManipulatorType.GetConstructor(new[] { typeof(IObjectDBTransaction), typeof(RelationInfo) }))
                .Ret();
            GenerateApartFieldsProperties(classImpl, interfaceType);
            var methods = RelationInfo.GetMethods(interfaceType);
            foreach (var method in methods)
            {
                if (method.Name.StartsWith("get_") || method.Name.StartsWith("set_"))
                    continue;
                var reqMethod = classImpl.DefineMethod("_R_" + method.Name, method.ReturnType,
                    method.GetParameters().Select(pi => pi.ParameterType).ToArray(), MethodAttributes.Virtual | MethodAttributes.Public);
                if (method.Name.StartsWith("RemoveBy"))
                {
                    if (method.Name == "RemoveByIdPartial")
                        BuildRemoveByIdPartialMethod(method, reqMethod, relationDBManipulatorType);
                    else
                        BuildRemoveByMethod(method, reqMethod, relationDBManipulatorType);
                }
                else if (method.Name.StartsWith("FindBy"))
                {
                    BuildFindByMethod(method, reqMethod, relationDBManipulatorType);
                }
                else if (method.Name == "Contains")
                {
                    BuildContainsMethod(method, reqMethod, relationDBManipulatorType);
                }
                else if (method.Name == "ListById") //list by primary key
                {
                    BuildListByIdMethod(method, reqMethod);
                }
                else if (method.Name.StartsWith("ListBy", StringComparison.Ordinal)) //ListBy{Name}(tenantId, .., AdvancedEnumeratorParam)
                {
                    BuildListByMethod(method, reqMethod);
                }
                else if (method.Name == "Insert")
                {
                    BuildInsertMethod(method, reqMethod, relationDBManipulatorType);
                }
                else
                {
                    BuildManipulatorCallWithSameParameters(method, reqMethod, relationDBManipulatorType);
                }
                reqMethod.Generator.Ret();
                classImpl.DefineMethodOverride(reqMethod, method);
            }
            var classImplType = classImpl.CreateType();

            return BuildRelationCreatorInstance<T>(classImplType, relationName, _relationInfo);
        }

        void BuildContainsMethod(MethodInfo method, IILMethod reqMethod, Type relationDBManipulatorType)
        {
            var writerLoc = reqMethod.Generator.DeclareLocal(typeof(ByteBufferWriter));
            reqMethod.Generator.Newobj(() => new ByteBufferWriter());
            reqMethod.Generator.Stloc(writerLoc);

            WriteShortPrefixIl(reqMethod.Generator, il => il.Ldloc(writerLoc), ObjectDB.AllRelationsPKPrefix);
            //ByteBufferWriter.WriteVUInt32(RelationInfo.Id);
            WriteIdIl(reqMethod.Generator, il => il.Ldloc(writerLoc), (int)_relationInfo.Id);
            var primaryKeyFields = _relationInfo.ClientRelationVersionInfo.GetPrimaryKeyFields();

            var count = SaveMethodParameters(reqMethod.Generator, "Contains", method.GetParameters(),
                method.GetParameters().Length,
                _relationInfo.ApartFields, primaryKeyFields, writerLoc);
            if (count != primaryKeyFields.Count)
                throw new BTDBException($"Number of parameters in Contains does not match primary key count {primaryKeyFields.Count}.");

            //call manipulator.Contains
            reqMethod.Generator
                .Ldarg(0); //manipulator
            //call byteBuffer.data
            var dataGetter = typeof(ByteBufferWriter).GetProperty("Data").GetGetMethod(true);
            reqMethod.Generator.Ldloc(writerLoc).Callvirt(dataGetter);
            reqMethod.Generator.Callvirt(relationDBManipulatorType.GetMethod("Contains"));
        }

        void BuildFindByMethod(MethodInfo method, IILMethod reqMethod, Type relationDBManipulatorType)
        {
            var writerLoc = reqMethod.Generator.DeclareLocal(typeof(ByteBufferWriter));
            reqMethod.Generator.Newobj(() => new ByteBufferWriter());
            reqMethod.Generator.Stloc(writerLoc);
            Action<IILGen> pushWriter = il => il.Ldloc(writerLoc);

            if (method.Name == "FindById" || method.Name == "FindByIdOrDefault")
            {
                CreateMethodFindById(reqMethod.Generator, relationDBManipulatorType, method.Name,
                    method.GetParameters(), method.ReturnType, _relationInfo.ApartFields, pushWriter, writerLoc);
            }
            else
            {
                CreateMethodFindBy(reqMethod.Generator, relationDBManipulatorType, method.Name, method.GetParameters(),
                    method.ReturnType, _relationInfo.ApartFields, pushWriter, writerLoc);
            }
        }

        void BuildRemoveByMethod(MethodInfo method, IILMethod reqMethod, Type relationDBManipulatorType)
        {
            var methodParameters = method.GetParameters();
            var isPrefixBased = method.ReturnType == typeof(int); //returns number of removed items

            var writerLoc = reqMethod.Generator.DeclareLocal(typeof(ByteBufferWriter));
            reqMethod.Generator.Newobj(() => new ByteBufferWriter());
            reqMethod.Generator.Stloc(writerLoc);
            Action<IILGen> pushWriter = il => il.Ldloc(writerLoc);

            if (isPrefixBased)
            {
                WriteShortPrefixIl(reqMethod.Generator, pushWriter, _relationInfo.Prefix);
            }
            else
            {
                WriteShortPrefixIl(reqMethod.Generator, pushWriter, ObjectDB.AllRelationsPKPrefix);
                //ByteBufferWriter.WriteVUInt32(RelationInfo.Id);
                WriteIdIl(reqMethod.Generator, pushWriter, (int) _relationInfo.Id);
            }

            var primaryKeyFields = _relationInfo.ClientRelationVersionInfo.GetPrimaryKeyFields();


            var count = SaveMethodParameters(reqMethod.Generator, method.Name, methodParameters, methodParameters.Length,
                _relationInfo.ApartFields, primaryKeyFields, writerLoc);
            if (!isPrefixBased && count != primaryKeyFields.Count)
                throw new BTDBException($"Number of parameters in {method.Name} does not match primary key count {primaryKeyFields.Count}.");

            //call manipulator.RemoveBy_
            reqMethod.Generator
                .Ldarg(0); //manipulator
            //call byteBuffer.data
            var dataGetter = typeof(ByteBufferWriter).GetProperty("Data").GetGetMethod(true);
            reqMethod.Generator.Ldloc(writerLoc).Callvirt(dataGetter);
            if (isPrefixBased)
            {
                if (AllKeyPrefixesAreSame(_relationInfo.ClientRelationVersionInfo, count) && !_relationInfo.NeedImplementFreeContent())
                    reqMethod.Generator.Callvirt(relationDBManipulatorType.GetMethod("RemoveByKeyPrefixWithoutIterate"));
                else
                    reqMethod.Generator.Callvirt(relationDBManipulatorType.GetMethod("RemoveByPrimaryKeyPrefix"));
            }
            else
            {
                reqMethod.Generator.LdcI4(ShouldThrowWhenKeyNotFound(method.Name, method.ReturnType) ? 1 : 0);
                reqMethod.Generator.Callvirt(relationDBManipulatorType.GetMethod("RemoveById"));
                if (method.ReturnType == typeof(void))
                    reqMethod.Generator.Pop();
            }
        }

        void BuildRemoveByIdPartialMethod(MethodInfo method, IILMethod reqMethod, Type relationDbManipulatorType)
        {
            var methodParameters = method.GetParameters();
            var isPrefixBased = method.ReturnType == typeof(int); //returns number of removed items

            if (!isPrefixBased || methodParameters.Length == 0 ||
                methodParameters[methodParameters.Length - 1].ParameterType != typeof(int) ||
                methodParameters[methodParameters.Length - 1].Name.IndexOf("max", StringComparison.InvariantCultureIgnoreCase) == -1)
            {
                throw new BTDBException("Invalid shape of RemoveByIdPartial.");
            }

            var il = reqMethod.Generator;
            var writerLoc = il.DeclareLocal(typeof(ByteBufferWriter));
            il
                .Newobj(() => new ByteBufferWriter())
                .Stloc(writerLoc);

            WriteShortPrefixIl(il, ilg => ilg.Ldloc(writerLoc), _relationInfo.Prefix);

            var primaryKeyFields = _relationInfo.ClientRelationVersionInfo.GetPrimaryKeyFields();
            SaveMethodParameters(il, method.Name, methodParameters, methodParameters.Length - 1,
                _relationInfo.ApartFields, primaryKeyFields, writerLoc);

            var dataGetter = typeof(ByteBufferWriter).GetProperty("Data").GetGetMethod(true);
            il
                .Ldarg(0) //manipulator
                .Ldloc(writerLoc).Callvirt(dataGetter)        //call byteBuffer.Data
                .Ldarg((ushort)methodParameters.Length)
                .Callvirt(relationDbManipulatorType.GetMethod("RemoveByPrimaryKeyPrefixPartial"));
        }

        static bool AllKeyPrefixesAreSame(RelationVersionInfo relationInfo, ushort count)
        {
            foreach (var sk in relationInfo.SecondaryKeys)
            {
                var skFields = sk.Value;
                var idx = 0;
                foreach (var field in skFields.Fields)
                {
                    if (!field.IsFromPrimaryKey)
                        return false;
                    if (field.Index != idx)
                        return false;
                    if (++idx == count)
                        break;
                }
            }
            return true;
        }

        void BuildListByIdMethod(MethodInfo method, IILMethod reqMethod)
        {
            var parameters = method.GetParameters();
            var advEnumParamOrder = (ushort)parameters.Length;
            var advEnumParam = parameters[advEnumParamOrder - 1].ParameterType;
            var advEnumParamType = advEnumParam.GenericTypeArguments[0];

            var emptyBufferLoc = reqMethod.Generator.DeclareLocal(typeof(ByteBuffer));
            var prefixParamCount = parameters.Length - 1;

            var primaryKeyFields = _relationInfo.ClientRelationVersionInfo.GetPrimaryKeyFields();
            var field = primaryKeyFields.Skip(_relationInfo.ApartFields.Count + prefixParamCount).First();
            ValidateAdvancedEnumParameter(field, advEnumParamType, method.Name);

            reqMethod.Generator
                .Ldarg(0);
            SavePKListPrefixBytes(reqMethod.Generator, method.Name,
                parameters, _relationInfo.ApartFields);
            reqMethod.Generator
                .LdcI4(prefixParamCount + _relationInfo.ApartFields.Count)
                .Ldarg(advEnumParamOrder).Ldfld(advEnumParam.GetField("Order"))
                .Ldarg(advEnumParamOrder).Ldfld(advEnumParam.GetField("StartProposition"));
            FillBufferWhenNotIgnoredKeyPropositionIl(advEnumParamOrder, field, emptyBufferLoc,
                advEnumParam.GetField("Start"), reqMethod.Generator);
            reqMethod.Generator
                .Ldarg(advEnumParamOrder).Ldfld(advEnumParam.GetField("EndProposition"));
            FillBufferWhenNotIgnoredKeyPropositionIl(advEnumParamOrder, field, emptyBufferLoc,
                advEnumParam.GetField("End"), reqMethod.Generator);

            if (typeof(IEnumerator<>).MakeGenericType(_relationInfo.ClientType).IsAssignableFrom(method.ReturnType))
            {
                //return new RelationAdvancedEnumerator<T>(relationManipulator,
                //    prefixBytes, prefixFieldCount,
                //    order,
                //    startKeyProposition, startKeyBytes,
                //    endKeyProposition, endKeyBytes, secondaryKeyIndex);
                var enumType = typeof(RelationAdvancedEnumerator<>).MakeGenericType(_relationInfo.ClientType);
                var advancedEnumeratorCtor = enumType.GetConstructors()[0];
                reqMethod.Generator.Newobj(advancedEnumeratorCtor);
            }
            else if (typeof(IOrderedDictionaryEnumerator<,>).MakeGenericType(advEnumParamType, _relationInfo.ClientType)
                .IsAssignableFrom(method.ReturnType))
            {
                reqMethod.Generator
                    .LdcI4(1); //init key reader

                //return new RelationAdvancedOrderedEnumerator<T>(relationManipulator,
                //    prefixBytes, prefixFieldCount,
                //    order,
                //    startKeyProposition, startKeyBytes,
                //    endKeyProposition, endKeyBytes, secondaryKeyIndex, initKeyReader);
                var enumType =
                    typeof(RelationAdvancedOrderedEnumerator<,>).MakeGenericType(advEnumParamType, _relationInfo.ClientType);
                var advancedEnumeratorCtor = enumType.GetConstructors()[0];
                reqMethod.Generator.Newobj(advancedEnumeratorCtor);
            }
            else
            {
                throw new BTDBException("Invalid method " + method.Name);
            }
        }

        void BuildListByMethod(MethodInfo method, IILMethod reqMethod)
        {
            var parameters = method.GetParameters();
            var advEnumParamOrder = (ushort)parameters.Length;
            var advEnumParam = parameters[advEnumParamOrder - 1].ParameterType;
            var advEnumParamType = advEnumParam.GenericTypeArguments[0];

            var emptyBufferLoc = reqMethod.Generator.DeclareLocal(typeof(ByteBuffer));
            var secondaryKeyIndex = _relationInfo.ClientRelationVersionInfo.GetSecondaryKeyIndex(method.Name.Substring(6));
            var prefixParamCount = parameters.Length - 1;

            var skFields = _relationInfo.ClientRelationVersionInfo.GetSecondaryKeyFields(secondaryKeyIndex);
            var field = skFields.Skip(_relationInfo.ApartFields.Count + prefixParamCount).First();
            ValidateAdvancedEnumParameter(field, advEnumParamType, method.Name);

            reqMethod.Generator
                .Ldarg(0);
            SaveListPrefixBytes(secondaryKeyIndex, reqMethod.Generator, method.Name,
                parameters, _relationInfo.ApartFields);
            reqMethod.Generator
                .LdcI4(prefixParamCount + _relationInfo.ApartFields.Count)
                .Ldarg(advEnumParamOrder).Ldfld(advEnumParam.GetField("Order"))
                .Ldarg(advEnumParamOrder).Ldfld(advEnumParam.GetField("StartProposition"));
            FillBufferWhenNotIgnoredKeyPropositionIl(advEnumParamOrder, field,
                emptyBufferLoc, advEnumParam.GetField("Start"), reqMethod.Generator);
            reqMethod.Generator
                .Ldarg(advEnumParamOrder).Ldfld(advEnumParam.GetField("EndProposition"));
            FillBufferWhenNotIgnoredKeyPropositionIl(advEnumParamOrder, field,
                emptyBufferLoc, advEnumParam.GetField("End"), reqMethod.Generator);
            reqMethod.Generator
                .LdcI4((int)secondaryKeyIndex);

            if (typeof(IEnumerator<>).MakeGenericType(_relationInfo.ClientType).IsAssignableFrom(method.ReturnType))
            {
                //return new RelationAdvancedSecondaryKeyEnumerator<T>(relationManipulator,
                //    prefixBytes, prefixFieldCount,
                //    order,
                //    startKeyProposition, startKeyBytes,
                //    endKeyProposition, endKeyBytes, secondaryKeyIndex);
                var enumType = typeof(RelationAdvancedSecondaryKeyEnumerator<>).MakeGenericType(_relationInfo.ClientType);
                var advancedEnumeratorCtor = enumType.GetConstructors()[0];
                reqMethod.Generator.Newobj(advancedEnumeratorCtor);
            }
            else if (typeof(IOrderedDictionaryEnumerator<,>).MakeGenericType(advEnumParamType, _relationInfo.ClientType)
                .IsAssignableFrom(method.ReturnType))
            {
                //return new RelationAdvancedOrderedSecondaryKeyEnumerator<T>(relationManipulator,
                //    prefixBytes, prefixFieldCount,
                //    order,
                //    startKeyProposition, startKeyBytes,
                //    endKeyProposition, endKeyBytes, secondaryKeyIndex);
                var enumType =
                    typeof(RelationAdvancedOrderedSecondaryKeyEnumerator<,>).MakeGenericType(advEnumParamType,
                        _relationInfo.ClientType);
                var advancedEnumeratorCtor = enumType.GetConstructors()[0];
                reqMethod.Generator.Newobj(advancedEnumeratorCtor);
            }
            else
            {
                throw new BTDBException("Invalid method " + method.Name);
            }
        }

        void ValidateAdvancedEnumParameter(TableFieldInfo field, Type advEnumParamType, string methodName)
        {
            if (!field.Handler.IsCompatibleWith(advEnumParamType, FieldHandlerOptions.Orderable))
            {
                throw new BTDBException($"Parameter type mismatch in {methodName} (expected '{field.Handler.HandledType().ToSimpleName()}' but '{advEnumParamType.ToSimpleName()}' found).");
            }
        }

        static void BuildInsertMethod(MethodInfo method, IILMethod reqMethod, Type relationDBManipulatorType)
        {
            var methodInfo = relationDBManipulatorType.GetMethod(method.Name);
            bool returningBoolVariant;
            var returnType = method.ReturnType;
            if (returnType == typeof(void))
                returningBoolVariant = false;
            else if (returnType == typeof(bool))
                returningBoolVariant = true;
            else
                throw new BTDBException("Method Insert should be defined with void or bool return type.");
            var methodParams = method.GetParameters();
            CheckParameterCount(method.Name, 1, methodParams.Length);
            CheckParameterType(method.Name, 0, methodInfo.GetParameters()[0].ParameterType, methodParams[0].ParameterType);
            reqMethod.Generator
                .Ldarg(0) //this
                .Ldarg(1)
                .Callvirt(methodInfo);
            if (!returningBoolVariant)
            {
                var returnedTrueLabel = reqMethod.Generator.DefineLabel("returnedTrueLabel");
                reqMethod.Generator
                    .Brtrue(returnedTrueLabel)
                    .Ldstr("Trying to insert duplicate key.")
                    .Newobj(() => new BTDBException(null))
                    .Throw()
                    .Mark(returnedTrueLabel);
            }
        }

        static void BuildManipulatorCallWithSameParameters(MethodInfo method, IILMethod reqMethod,
            Type relationDBManipulatorType)
        {
            var methodParams = method.GetParameters();
            int paramCount = methodParams.Length;
            var methodInfo = relationDBManipulatorType.GetMethod(method.Name);
            if (methodInfo == null)
                throw new BTDBException($"Method {method} is not supported.");
            CheckReturnType(method.Name, methodInfo.ReturnType, method.ReturnType);
            var calledMethodParams = methodInfo.GetParameters();
            CheckParameterCount(method.Name, calledMethodParams.Length, methodParams.Length);
            for (int i = 0; i < methodParams.Length; i++)
            {
                CheckParameterType(method.Name, i, calledMethodParams[i].ParameterType, methodParams[i].ParameterType);
            }
            for (ushort i = 0; i <= paramCount; i++)
                reqMethod.Generator.Ldarg(i);
            reqMethod.Generator.Callvirt(methodInfo);
        }

        static void CheckParameterType(string name, int parIdx, Type expectedType, Type actualType)
        {
            if (expectedType != actualType)
                throw new BTDBException($"Method {name} expects {parIdx}th parameter of type {expectedType.Name}.");
        }

        static void CheckParameterCount(string name, int expectedParameterCount, int actualParameterCount)
        {
            if (expectedParameterCount != actualParameterCount)
                throw new BTDBException($"Method {name} expects {expectedParameterCount} parameters count.");
        }

        static void CheckReturnType(string name, Type expectedReturnType, Type returnType)
        {
            if (returnType != expectedReturnType)
                throw new BTDBException($"Method {name} should be defined with {expectedReturnType.Name} return type.");
        }

        static Func<IObjectDBTransaction, T1> BuildRelationCreatorInstance<T1>(Type classImplType, string relationName, RelationInfo relationInfo)
        {
            var methodBuilder = ILBuilder.Instance.NewMethod("RelationFactory" + relationName, typeof(Func<IObjectDBTransaction, T1>), typeof(RelationInfo));
            var ilGenerator = methodBuilder.Generator;
            ilGenerator
                .Ldarg(1)
                .Ldarg(0)
                .Newobj(classImplType.GetConstructor(new[] { typeof(IObjectDBTransaction), typeof(RelationInfo) }))
                .Castclass(typeof(T1))
                .Ret();
            return (Func<IObjectDBTransaction, T1>)methodBuilder.Create(relationInfo);
        }

        void CreateMethodFindById(IILGen ilGenerator, Type relationDBManipulatorType, string methodName,
                        ParameterInfo[] methodParameters, Type methodReturnType, IDictionary<string, MethodInfo> apartFields, Action<IILGen> pushWriter,
                        IILLocal writerLoc)
        {
            var isPrefixBased = ReturnsEnumerableOfClientType(methodReturnType, _relationInfo.ClientType);
            if (isPrefixBased)
            {
                WriteShortPrefixIl(ilGenerator, pushWriter, _relationInfo.Prefix);
            }
            else
            {
                WriteShortPrefixIl(ilGenerator, pushWriter, ObjectDB.AllRelationsPKPrefix);
                //ByteBufferWriter.WriteVUInt32(RelationInfo.Id);
                WriteIdIl(ilGenerator, pushWriter, (int) _relationInfo.Id);
            }

            var primaryKeyFields = _relationInfo.ClientRelationVersionInfo.GetPrimaryKeyFields();

            var count = SaveMethodParameters(ilGenerator, methodName, methodParameters, methodParameters.Length,
                apartFields, primaryKeyFields, writerLoc);
            if (!isPrefixBased && count != primaryKeyFields.Count)
                throw new BTDBException(
                    $"Number of parameters in {methodName} does not match primary key count {primaryKeyFields.Count}.");

            //call manipulator.FindBy_
            ilGenerator
                .Ldarg(0); //manipulator
            //call byteBuffer.data
            var dataGetter = typeof(ByteBufferWriter).GetProperty("Data").GetGetMethod(true);
            ilGenerator.Ldloc(writerLoc).Call(dataGetter);
            if (isPrefixBased)
            {
                ilGenerator.Callvirt(relationDBManipulatorType.GetMethod("FindByPrimaryKeyPrefix"));
            }
            else
            {
                ilGenerator.LdcI4(ShouldThrowWhenKeyNotFound(methodName, methodReturnType) ? 1 : 0);
                ilGenerator.Callvirt(relationDBManipulatorType.GetMethod("FindByIdOrDefault"));
                if (methodReturnType == typeof(void))
                    ilGenerator.Pop();
            }
        }

        void CreateMethodFindBy(IILGen ilGenerator, Type relationDBManipulatorType, string methodName,
            ParameterInfo[] methodParameters, Type methodReturnType, IDictionary<string, MethodInfo> apartFields, Action<IILGen> pushWriter,
            IILLocal writerLoc)
        {
            bool allowDefault = false;
            var skName = methodName.Substring(6);
            if (skName.EndsWith("OrDefault"))
            {
                skName = skName.Substring(0, skName.Length - 9);
                allowDefault = true;
            }

            WriteShortPrefixIl(ilGenerator, pushWriter, ObjectDB.AllRelationsSKPrefix);
            var skIndex = _relationInfo.ClientRelationVersionInfo.GetSecondaryKeyIndex(skName);
            //ByteBuffered.WriteVUInt32(RelationInfo.Id);
            WriteIdIl(ilGenerator, pushWriter, (int)_relationInfo.Id);
            //ByteBuffered.WriteVUInt32(skIndex);
            WriteIdIl(ilGenerator, pushWriter, (int)skIndex);

            var secondaryKeyFields = _relationInfo.ClientRelationVersionInfo.GetSecondaryKeyFields(skIndex);
            SaveMethodParameters(ilGenerator, methodName, methodParameters, methodParameters.Length,
                apartFields, secondaryKeyFields, writerLoc);

            //call public T FindBySecondaryKeyOrDefault(uint secondaryKeyIndex, uint prefixParametersCount, ByteBuffer secKeyBytes, bool throwWhenNotFound)
            ilGenerator.Ldarg(0); //manipulator
            ilGenerator.LdcI4((int)skIndex);
            ilGenerator.LdcI4(methodParameters.Length + apartFields.Count);
            //call byteBuffer.data
            var dataGetter = typeof(ByteBufferWriter).GetProperty("Data").GetGetMethod(true);
            ilGenerator.Ldloc(writerLoc).Callvirt(dataGetter);
            if (ReturnsEnumerableOfClientType(methodReturnType, _relationInfo.ClientType))
            {
                ilGenerator.Callvirt(relationDBManipulatorType.GetMethod("FindBySecondaryKey"));
            }
            else
            {
                ilGenerator.LdcI4(allowDefault ? 0 : 1); //? should throw
                ilGenerator.Callvirt(relationDBManipulatorType.GetMethod("FindBySecondaryKeyOrDefault"));
            }
        }

        static bool ReturnsEnumerableOfClientType(Type methodReturnType, Type clientType)
        {
            return methodReturnType.IsGenericType &&
                   methodReturnType.GetGenericTypeDefinition() == typeof(IEnumerator<>) &&
                   methodReturnType.GetGenericArguments()[0] == clientType;
        }

        static ushort SaveMethodParameters(IILGen ilGenerator, string methodName,
                                           ParameterInfo[] methodParameters, int paramCount,
                                           IDictionary<string, MethodInfo> apartFields,
                                           IEnumerable<TableFieldInfo> fields, IILLocal writerLoc)
        {
            ushort usedApartFieldsCount = 0;
            ushort idx = 0;
            foreach (var field in fields)
            {
                MethodInfo fieldGetter;
                if (apartFields.TryGetValue(field.Name, out fieldGetter))
                {
                    usedApartFieldsCount++;
                    SaveKeyFieldFromApartField(ilGenerator, field, fieldGetter, writerLoc);
                    continue;
                }
                if (idx == paramCount)
                {
                    break;
                }
                var par = methodParameters[idx++];
                if (string.Compare(field.Name, par.Name.ToLower(), StringComparison.OrdinalIgnoreCase) != 0)
                {
                    throw new BTDBException($"Parameter and key mismatch in {methodName}, {field.Name}!={par.Name}.");
                }
                if (!field.Handler.IsCompatibleWith(par.ParameterType, FieldHandlerOptions.Orderable))
                {
                    throw new BTDBException($"Parameter type mismatch in {methodName} (expected '{field.Handler.HandledType().ToSimpleName()}' but '{par.ParameterType.ToSimpleName()}' found).");
                }
                SaveKeyFieldFromArgument(ilGenerator, field, idx, par.ParameterType, writerLoc);
            }
            if (usedApartFieldsCount != apartFields.Count)
            {
                throw new BTDBException($"Apart fields must be part of prefix in {methodName}.");
            }
            return (ushort)(idx + usedApartFieldsCount);
        }

        static bool ShouldThrowWhenKeyNotFound(string methodName, Type methodReturnType)
        {
            if (methodName.StartsWith("RemoveBy"))
                return methodReturnType == typeof(void);
            if (methodName == "FindByIdOrDefault")
                return false;
            return true;
        }

        internal static void FillBufferWhenNotIgnoredKeyPropositionIl(ushort advEnumParamOrder, TableFieldInfo field, IILLocal emptyBufferLoc,
                                                               FieldInfo instField, IILGen ilGenerator)
        {
            //stack contains KeyProposition
            var ignoreLabel = ilGenerator.DefineLabel(instField + "_ignore");
            var doneLabel = ilGenerator.DefineLabel(instField + "_done");
            var writerLoc = ilGenerator.DeclareLocal(typeof(AbstractBufferedWriter));
            ilGenerator
                .Dup()
                .LdcI4((int)KeyProposition.Ignored)
                .BeqS(ignoreLabel)
                .Newobj(() => new ByteBufferWriter())
                .Stloc(writerLoc);
            field.Handler.SpecializeSaveForType(instField.FieldType).Save(ilGenerator,
                il => il.Ldloc(writerLoc),
                il => il.Ldarg(advEnumParamOrder).Ldfld(instField));
            var dataGetter = typeof(ByteBufferWriter).GetProperty("Data").GetGetMethod(true);
            ilGenerator.Ldloc(writerLoc).Castclass(typeof(ByteBufferWriter)).Callvirt(dataGetter);
            ilGenerator
                .Br(doneLabel)
                .Mark(ignoreLabel)
                .Ldloc(emptyBufferLoc)
                .Mark(doneLabel);
        }

        void SaveListPrefixBytes(uint secondaryKeyIndex, IILGen ilGenerator, string methodName, ParameterInfo[] methodParameters,
            IDictionary<string, MethodInfo> apartFields)
        {
            var writerLoc = ilGenerator.DeclareLocal(typeof(ByteBufferWriter));
            ilGenerator
                .Newobj(() => new ByteBufferWriter())
                .Stloc(writerLoc);

            Action<IILGen> pushWriter = il => il.Ldloc(writerLoc);

            WriteShortPrefixIl(ilGenerator, pushWriter, ObjectDB.AllRelationsSKPrefix);
            //ByteBuffered.WriteVUInt32(RelationInfo.Id);
            WriteIdIl(ilGenerator, pushWriter, (int)_relationInfo.Id);
            //ByteBuffered.WriteVUInt32(skIndex);
            WriteIdIl(ilGenerator, pushWriter, (int)secondaryKeyIndex);

            var secondaryKeyFields = _relationInfo.ClientRelationVersionInfo.GetSecondaryKeyFields(secondaryKeyIndex);
            var paramCount = methodParameters.Length - 1; //last param is key proposition
            SaveMethodParameters(ilGenerator, methodName, methodParameters, paramCount, apartFields,
                secondaryKeyFields, writerLoc);

            var dataGetter = typeof(ByteBufferWriter).GetProperty("Data").GetGetMethod(true);
            ilGenerator.Ldloc(writerLoc).Callvirt(dataGetter);
        }

        void SavePKListPrefixBytes(IILGen ilGenerator, string methodName, ParameterInfo[] methodParameters,
                                         IDictionary<string, MethodInfo> apartFields)
        {
            var writerLoc = ilGenerator.DeclareLocal(typeof(ByteBufferWriter));
            ilGenerator
                .Newobj(() => new ByteBufferWriter())
                .Stloc(writerLoc);

            Action<IILGen> pushWriter = il => il.Ldloc(writerLoc);
            WriteShortPrefixIl(ilGenerator, pushWriter, _relationInfo.Prefix);

            var keyFields = _relationInfo.ClientRelationVersionInfo.GetPrimaryKeyFields();
            var paramCount = methodParameters.Length - 1; //last param is key proposition
            SaveMethodParameters(ilGenerator, methodName, methodParameters, paramCount, apartFields,
                keyFields, writerLoc);

            var dataGetter = typeof(ByteBufferWriter).GetProperty("Data").GetGetMethod(true);
            ilGenerator.Ldloc(writerLoc).Callvirt(dataGetter);
        }

        void GenerateApartFieldsProperties(IILDynamicType classImpl, Type interfaceType)
        {
            var apartFields = new Dictionary<string, IILField>();
            var initializedFields = new Dictionary<string, IILField>();
            var methods = RelationInfo.GetMethods(interfaceType);
            var properties = RelationInfo.GetProperties(interfaceType).ToArray();
            foreach (var method in methods)
            {
                var name = method.Name;
                if (!name.StartsWith("get_") && !name.StartsWith("set_"))
                    continue;

                IILField field;
                IILField initCheckField;
                var propName = RelationInfo.GetPersistentName(method.Name.Substring(4), properties);

                if (!_relationInfo.ApartFields.ContainsKey(propName))
                    throw new BTDBException($"Invalid property name {propName}.");

                if (!apartFields.TryGetValue(propName, out field))
                {
                    field = classImpl.DefineField("_" + propName, method.ReturnType, FieldAttributes.Private);
                    apartFields[propName] = field;
                    initCheckField = classImpl.DefineField("_initialized_" + propName, typeof(bool), FieldAttributes.Private);
                    initializedFields[propName] = initCheckField;
                }
                else
                {
                    initCheckField = initializedFields[propName];
                }

                var reqMethod = classImpl.DefineMethod(method.Name, method.ReturnType,
                    method.GetParameters().Select(pi => pi.ParameterType).ToArray(),
                    MethodAttributes.Virtual | MethodAttributes.Public);
                if (name.StartsWith("set_"))
                {
                    reqMethod.Generator.Ldarg(0).Ldarg(1).Stfld(field)
                        .Ldarg(0).LdcI4(1).Stfld(initCheckField)
                        .Ret();
                }
                else
                {
                    var initializedLabel = reqMethod.Generator.DefineLabel("initialized");
                    reqMethod.Generator
                        .Ldarg(0).Ldfld(initCheckField)
                        .Brtrue(initializedLabel)
                        .Ldstr($"Cannot use uninitialized apart field {propName}")
                        .Newobj(() => new BTDBException(null))
                        .Throw()
                        .Mark(initializedLabel)
                        .Ldarg(0).Ldfld(field).Ret();
                }
                classImpl.DefineMethodOverride(reqMethod, method);
            }
        }

        static void SaveKeyFieldFromArgument(IILGen ilGenerator, TableFieldInfo field, ushort parameterId, Type parameterType, IILLocal writerLoc)
        {
            field.Handler.SpecializeSaveForType(parameterType).Save(ilGenerator,
                il => il.Ldloc(writerLoc),
                il => il.Ldarg(parameterId));
        }

        static void SaveKeyFieldFromApartField(IILGen ilGenerator, TableFieldInfo field, MethodInfo fieldGetter, IILLocal writerLoc)
        {
            field.Handler.SpecializeSaveForType(fieldGetter.ReturnType).Save(ilGenerator,
                il => il.Ldloc(writerLoc),
                il => il.Ldarg(0).Callvirt(fieldGetter));
        }

        static void WriteIdIl(IILGen ilGenerator, Action<IILGen> pushWriter, int id)
        {
            var bytes = new byte[PackUnpack.LengthVUInt((uint)id)];
            int o = 0;
            PackUnpack.PackVUInt(bytes, ref o, (uint)id);
            WriteShortPrefixIl(ilGenerator, pushWriter, bytes);
        }

        static void WriteShortPrefixIl(IILGen ilGenerator, Action<IILGen> pushWriter, byte[] prefix)
        {
            foreach (byte b in prefix)
                ilGenerator
                    .Do(pushWriter)
                    .LdcI4(b)
                    .Call(() => default(AbstractBufferedWriter).WriteUInt8(0));
        }

    }
}