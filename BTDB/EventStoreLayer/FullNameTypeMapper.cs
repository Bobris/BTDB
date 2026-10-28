using System;
using System.Collections.Generic;
using System.Text;

namespace BTDB.EventStoreLayer;

[Generate]
public class FullNameTypeMapper : ITypeNameMapper
{
    public string ToName(Type type)
    {
        if (!type.IsGenericType)
            return type.FullName;

        var sb = new StringBuilder();
        ToName(type, sb);
        return sb.ToString();
    }

    void ToName(Type type, StringBuilder sb)
    {
        if (type.IsGenericType)
        {
            sb
                .Append(type.FullName, 0, type.FullName.IndexOf('`'))
                .Append('<');

            var args = type.GetGenericArguments();
            for (int i = 0; i < args.Length; i++)
            {
                if (i != 0)
                    sb.Append(',');
                ToName(args[i], sb);
            }

            sb.Append('>');
        }
        else
        {
            sb.Append(type.FullName);
        }
    }

    enum TokenType
    {
        Left,
        Type,
        GenericType
    }

    class Token
    {
        public TokenType TokenType;
        public Type Type;
        public int Start;
        public int End;
    }

    public Type ToType(string name)
    {
        int i = name.IndexOf('<');
        if (i == -1)
            return ToTypeInternal(name);

        var stack = new Stack<Token>();
        stack.Push(new Token { TokenType = TokenType.GenericType, End = i });
        stack.Push(new Token { TokenType = TokenType.Left });
        stack.Push(new Token { TokenType = TokenType.Type, Start = i + 1 });

        while (++i < name.Length)
        {
            // process all previous types till we hit '<' on stack
            if (name[i] == '>')
            {
                stack.Peek().End = i;

                var args = new List<Token>();
                while (stack.Count > 0 && stack.Peek().TokenType != TokenType.Left)
                    args.Add(stack.Pop());
                // A name with unbalanced brackets, as damage leaves, names no type.
                if (stack.Count < 2) return null;
                stack.Pop();

                int arity = args.Count;
                var token = stack.Peek();
                var typeName = name.Substring(token.Start, token.End - token.Start);
                var genericDefinition = ToTypeInternal(typeName + '`' + arity);
                if (genericDefinition == null) return null;

                var typeArgs = new Type[arity];
                for (int j = 0; j < arity; j++)
                {
                    var argToken = args[arity - j - 1];
                    if (argToken.TokenType == TokenType.GenericType)
                        typeArgs[j] = argToken.Type;
                    else
                    {
                        var argTypeName = name.Substring(argToken.Start, argToken.End - argToken.Start);
                        typeArgs[j] = ToTypeInternal(argTypeName);
                    }

                    // A type argument that maps to no type, as after it was renamed, makes the whole name map to none.
                    if (typeArgs[j] == null) return null;
                }

                token.Type = genericDefinition.MakeGenericType(typeArgs);
            }
            // probably previous type end set end on last token
            else if (name[i] == ',')
            {
                if (stack.Peek().TokenType != TokenType.GenericType)
                    stack.Peek().End = i;
                stack.Push(new Token { TokenType = TokenType.Type, Start = i + 1 });
            }
            // create new generic type
            else if (name[i] == '<')
            {
                if (stack.Peek().TokenType != TokenType.Type) return null;
                stack.Peek().TokenType = TokenType.GenericType;
                stack.Peek().End = i;
                stack.Push(new Token { TokenType = TokenType.Left });
                stack.Push(new Token { TokenType = TokenType.Type, Start = i + 1 });
            }
        }

        return stack.Count == 1 ? stack.Pop().Type : null;
    }

    Type ToTypeInternal(string name)
    {
        var t = Type.GetType(name);
        if (t != null)
            return t;
        foreach (var asm in AppDomain.CurrentDomain.GetAssemblies())
        {
            t = asm.GetType(name);
            if (t != null)
                return t;
        }

        return null;
    }
}
