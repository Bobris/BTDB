using System;
using System.Reflection.Emit;
using BTDB.IL;
using Xunit;

namespace BTDBTest.ILBuilderTest
{
    public class ILBuilderTest
    {
        [Fact]
        public void CanCreateConstructorWithNamedParameter()
        {
            var typeBuilder = ILBuilder.Instance.NewType("test", null, null);
            var ctor = typeBuilder.DefineConstructor(new[] {typeof(int)}, new []{"answer"});
            ctor.Generator.Emit(OpCodes.Ret);
            var type = typeBuilder.CreateType();

            Assert.NotNull(Activator.CreateInstance(type, 42));
            var generatedCtors = type.GetConstructors();
            Assert.Single(generatedCtors);
            var parameters = generatedCtors[0].GetParameters();
            Assert.Single(parameters);
            Assert.Equal("answer", parameters[0].Name);
        }
    }
}