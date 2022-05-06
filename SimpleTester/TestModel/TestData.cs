using System;
using System.Collections.Generic;
using SimpleTester.TestModel.Events;

namespace SimpleTester.TestModel;

public static class TestData
{
    public static NewUserEvent SimpleEventInstance() => new NewUserEvent
    {
        Id = 123456,
        ParentEventId = 0,
        Time = new DateTime(2016, 4, 22, 14, 55, 25, DateTimeKind.Utc),
        UniqueGuid = new Guid("FD76C45F-3991-4D48-9BEB-B2DB25B6EB11"),
        Name = "Boris Letocha",
        Password = new byte[20]
    };

    public static TemplateSavedV1 ComplexEventInstance() => new TemplateSavedV1
    {
        ActionId = 666,
        Result = "Lorem ipsum",
        Results = new[] { "L1", "L2" },
        SessionId = "E95BA224-FAF8-4056-ADEC-1E6E0FBAED1B",
        Name = "Foobar",
        CompanyId = 5555,
        ForceSave = true,
        Id = 98765432123456789,
        IsCreating = true,
        LogoId = 6666,
        OldTemplateId = 7777,
        SavedFromJobId = 88888,
        TemplateId = 9999,
        Time = new DateTime(2016, 4, 22, 14, 55, 25, DateTimeKind.Utc),
        UniqueGuid = new Guid("FD76C45F-3991-4D48-9BEB-B2DB25B6EB11"),
        UserId = 1000,
        WindowId = "abeceda",
        Configuration = new TemplateConfiguration
        {
            ExtraData = "EXTRA",
            CompanyBrand = new CompanyBrand
            {
                ContentBackgroundColor = "FF00FF",
                ContentButtonColor = "00FF00",
                ContentButtonTextColor = "0F0",
                FontFamily = "Tahoma",
                HeaderBackgroundColor = "FFAAFF",
                HeaderFont = new FontSettings { Color = "red", Size = 5.6, Style = "italic" },
                TextFont = new FontSettings { Color = "blue", Size = 6.6, Style = "italic" },
                TitleFont = new FontSettings { Color = "gree", Size = 7.6, Style = "italic" }
            },
            DifferentCompanyBrands = new Dictionary<string, CompanyBrand>
            {
                ["cz"] = new CompanyBrand
                {
                    FontFamily = "CZ-Tahoma"
                },
                ["en"] = new CompanyBrand
                {
                    FontFamily = "EN-Tahoma"
                }
            },
            Invitation = new Invitation
            {
                Content = @"Lorem ipsum dolor sit amet,
                        consectetur adipiscing elit.Pellentesque nec ipsum sapien.Etiam molestie dui vitae augue maximus,
                        malesuada maximus ante interdum.Fusce non diam rutrum,
                        dictum magna in,
                        pharetra quam.Fusce pharetra ac elit quis pharetra.Praesent non sem tempus elit egestas lobortis.Cras cursus,
                        turpis a elementum dignissim,
                        nisl purus bibendum quam,
                        et ornare arcu lectus quis libero.Mauris pharetra ipsum nec ornare venenatis.Praesent at mollis neque.Praesent suscipit elit ex,
                        maximus pretium ipsum porta id.Etiam finibus,
                        lacus vitae lobortis aliquam,
                        est dolor egestas mauris,
                        tempor vehicula ante erat ut dolor.Donec finibus,
                        risus eget efficitur blandit,
                        enim nisl imperdiet erat,
                        blandit vulputate tellus velit sed enim.
Duis sit amet nisi a enim mollis blandit.Cras maximus risus mauris,
                        at condimentum mauris placerat in. Praesent nec sapien nisl.Phasellus faucibus elit eget iaculis dictum.Phasellus pretium elit vehicula,
                        placerat ante vitae,
                        auctor dolor.Vivamus pretium elementum pulvinar.Quisque in nibh justo.Vivamus vel commodo lectus.Etiam eu massa non turpis auctor rutrum at sit amet erat.In nec mattis ligula,
                        ac aliquet nisl.Donec quam arcu,
                        dignissim id feugiat et,
                        luctus sit amet nulla.Integer cursus lacus nunc,
                        ut elementum sem condimentum non.Duis maximus lobortis ante,
                        euismod sagittis tortor molestie ut.
Maecenas at felis dapibus,
                        sollicitudin sapien vitae,
                        tincidunt est.Nulla eleifend ultrices diam.Phasellus iaculis imperdiet mi,
                        ac tincidunt urna vestibulum ac.Pellentesque elementum tempor quam eu vulputate.Suspendisse condimentum ac nisi sed lobortis.Mauris blandit,
                        odio vel pretium tempus,
                        magna tortor aliquam nisl,
                        nec hendrerit nibh ligula finibus ligula.Nunc vel faucibus lectus.Fusce at sapien ac nisi commodo tempus.Aenean sed odio sem.Sed pharetra rhoncus metus,
                        vitae scelerisque ipsum sollicitudin vel.Donec quis varius lorem.
Donec tortor nulla,
                        volutpat ac odio quis,
                        rutrum dictum magna.Aenean fringilla elementum metus,
                        id consectetur risus porta non.Maecenas semper efficitur nibh.Fusce lorem dolor,
                        ornare eget nibh sit amet,
                        dapibus efficitur turpis.Duis pharetra ipsum nec tellus dignissim,
                        id semper diam blandit.Sed eros nisi,
                        aliquet eu turpis vel,
                        vestibulum tempor est.Maecenas volutpat nisi et maximus fermentum.Duis eu semper arcu.
Morbi non turpis a ante placerat dapibus.Vestibulum purus tortor,
                        blandit vel congue et,
                        volutpat eget nunc.Quisque laoreet malesuada ante,
                        vel ornare odio dapibus ac.Etiam consectetur,
                        risus ut pulvinar molestie,
                        turpis tortor dictum odio,
                        quis egestas sem ipsum non erat.Nulla facilisi.Nulla orci odio,
                        convallis eu laoreet vel,
                        ultricies id sem.Maecenas accumsan augue nec tempus laoreet.Mauris eget luctus eros.Sed quis odio sit amet mauris placerat facilisis ornare in erat.Proin ut dolor est.Donec sollicitudin ipsum vel sem iaculis,
                        nec pellentesque ipsum tristique.Fusce maximus eros ligula,
                        vel dictum odio lobortis vitae.Class aptent taciti sociosqu ad litora torquent per conubia nostra,
                        per inceptos himenaeos.Vestibulum eget porttitor elit.Vivamus non nulla sed ante efficitur condimentum.In maximus euismod neque,
                        et tristique augue egestas sit amet. ",
                SenderEmail = "foo@example.org",
                SenderName = "Foo",
                Subject = "Bar",
                Attachment = new byte[] { 0xc0, 0xff, 0xee }
            },
            Variables = new[] { new Variable("Name", "John Doe"), new Variable("Email", "foo@example.org") },
            Language = Language.EnUk,
            Languages = Languages.De | Languages.EnUk,
            Preference = new Preference
            {
                Content = new PageContentWithButton
                {
                    Message = "Bububu",
                    SendButtonText = "Send",
                    Title = "FooBar?"
                },
                Confirmation = new PageContent
                {
                    Message = "OK",
                    Title = "OK?"
                },
                Heading = "Lorem ipsum",
                Options = new List<Option> { new Option { Value = "Opt1" }, new Option { Value = "Opt2" } },
                OptionsLookup = new Dictionary<string, Option>
                {
                    ["1"] = new Option { Value = "1" },
                    ["2"] = new Option { Value = "2" }
                },
                TermsAndConditions =
                    new TermsAndConditions
                    {
                        Message = "OK?",
                        Use = true
                    }
            }
        }
    };
}
