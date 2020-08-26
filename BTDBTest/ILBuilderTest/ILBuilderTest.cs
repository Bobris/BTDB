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
            Assert.Equal(1, generatedCtors.Length);
            var parameters = generatedCtors[0].GetParameters();
            Assert.Equal(1, parameters.Length);
            Assert.Equal("answer", parameters[0].Name);
        }
    }
}