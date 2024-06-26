using Dynamicweb.Data;
using Dynamicweb.DataIntegration.Integration;
using Dynamicweb.DataIntegration.Integration.Interfaces;
using Dynamicweb.DataIntegration.ProviderHelpers;
using Dynamicweb.Extensibility.AddIns;
using Dynamicweb.Extensibility.Editors;
using Dynamicweb.Logging;
using System;
using System.Collections.Generic;
using System.Collections.ObjectModel;
using System.Data;
using System.Data.SqlClient;
using System.Globalization;
using System.Linq;
using System.Xml;
using System.Xml.Linq;

namespace Dynamicweb.DataIntegration.Providers.EcomProvider;

[AddInName("Dynamicweb.DataIntegration.Providers.Provider"), AddInLabel("Ecom Provider"), AddInDescription("Ecom provider"), AddInIgnore(false)]
public class EcomProvider : BaseSqlProvider, IParameterOptions, IParameterVisibility, ISource, IDestination
{
    private Schema? Schema;
    private bool IsFirstJobRun = true;

    #region Source AddIns
    [AddInParameter("Source language")]
    [AddInParameterEditor(typeof(DropDownParameterEditor), "none=true;Tooltip=Select only products in this language.")]
    [AddInParameterGroup("Source")]
    public string SourceLanguage { get; set; } = "";

    [AddInParameter("Source shop")]
    [AddInParameterEditor(typeof(DropDownParameterEditor), "none=true;Tooltip=Select only products from this shop.")]
    [AddInParameterGroup("Source")]
    public string SourceShop { get; set; } = "";

    [AddInParameter("Get groups for variant options by:")]
    [AddInParameterEditor(typeof(RadioParameterEditor), "")]
    [AddInParameterGroup("Source")]
    public string GroupsForVariantOptionsBy
    {
        get
        {
            return GetGroupNamesForVariantOptions ? "Name" : "ID";
        }
        set
        {
            GetGroupNamesForVariantOptions = value == "Name";
        }
    }
    public bool GetGroupNamesForVariantOptions { get; set; }

    [AddInParameter("Get manufacturer for products by:")]
    [AddInParameterEditor(typeof(RadioParameterEditor), "")]
    [AddInParameterGroup("Source")]
    public string ManufacturerForProductsBy
    {
        get
        {
            return GetManufacturerNamesForProducts ? "Name" : "ID";
        }
        set
        {
            GetManufacturerNamesForProducts = value == "Name";
        }

    }
    public bool GetManufacturerNamesForProducts { get; set; }

    [AddInParameter("Get variant groups for products by:")]
    [AddInParameterEditor(typeof(RadioParameterEditor), "")]
    [AddInParameterGroup("Source")]
    public string VariantGroupsForProductsBy
    {
        get
        {
            return GetVariantGroupNamesForProduct ? "Name" : "ID";
        }
        set { GetVariantGroupNamesForProduct = value == "Name"; }
    }
    public bool GetVariantGroupNamesForProduct { get; set; }

    [AddInParameter("Get groups for products by:")]
    [AddInParameterEditor(typeof(RadioParameterEditor), "")]
    [AddInParameterGroup("Source")]
    public string GroupsForProductsBy
    {
        get
        {
            return GetGroupNamesForProduct ? "Name" : "ID";
        }
        set { GetGroupNamesForProduct = value == "Name"; }
    }
    public bool GetGroupNamesForProduct { get; set; }

    [AddInParameter("Get related products by:")]
    [AddInParameterEditor(typeof(RadioParameterEditor), "")]
    [AddInParameterGroup("Source")]
    public string RelatedProductsBy
    {
        get
        {
            return GetRelatedProductsByName ? "Name" : "ID";
        }
        set { GetRelatedProductsByName = value == "Name"; }
    }
    public bool GetRelatedProductsByName { get; set; }

    [AddInParameter("Get related product groups by:")]
    [AddInParameterEditor(typeof(RadioParameterEditor), "")]
    [AddInParameterGroup("Source")]
    public string RelatedProductGroupsBy
    {
        get
        {
            return GetRelatedProductGroupsByName ? "Name" : "ID";
        }
        set { GetRelatedProductGroupsByName = value == "Name"; }
    }
    public bool GetRelatedProductGroupsByName { get; set; }
    #endregion

    #region Destination AddIns
    private string? defaultLanguage;
    [AddInParameter("Default Language")]
    [AddInParameterEditor(typeof(DropDownParameterEditor), "none=true;Tooltip=Set the default language for the imported products")]
    [AddInParameterGroup("Destination")]
    [AddInParameterOrder(10)]
    public string DefaultLanguage
    {
        get
        {
            if (defaultLanguage == null)
            {
                defaultLanguage = Ecommerce.Services.Languages.GetDefaultLanguageId();
            }
            return defaultLanguage;
        }
        set
        {
            defaultLanguage = value;
        }
    }

    [AddInParameter("Shop")]
    [AddInParameterEditor(typeof(DropDownParameterEditor), "none=true;Tooltip=Set a shop for the imported products")]
    [AddInParameterGroup("Destination")]
    [AddInParameterOrder(20)]
    public string Shop { get; set; } = "";

    [AddInParameter("Insert only new records")]
    [AddInParameterEditor(typeof(YesNoParameterEditor), "Tooltip=Inserts new records present in the source, but does not update existing records")]
    [AddInParameterGroup("Destination")]
    [AddInParameterOrder(23)]
    public bool InsertOnlyNewRecords { get; set; }

    [AddInParameter("Update only existing records")]
    [AddInParameterEditor(typeof(YesNoParameterEditor), "Tooltip=When this option is ON the imported rows are updated but not inserted. When OFF rows are updated and inserted")]
    [AddInParameterGroup("Destination")]
    [AddInParameterOrder(25)]
    public bool UpdateOnlyExistingRecords { get; set; }

    [AddInParameter("Deactivate missing products")]
    [AddInParameterEditor(typeof(YesNoParameterEditor), "Tooltip=When ON missing products are deactivated. When OFF no action is taken. When Delete incoming rows is ON, Deactivate missing products is skipped. The Hide deactivated products option is used only when Deactivate missing products is ON")]
    [AddInParameterGroup("Destination")]
    [AddInParameterOrder(30)]
    public bool DeactivateMissingProducts { get; set; }

    [AddInParameter("Remove missing rows after import in the destination tables only")]
    [AddInParameterEditor(typeof(YesNoParameterEditor), "Tooltip=Deletes rows not present in the import source - excluding related tabled")]
    [AddInParameterGroup("Destination")]
    [AddInParameterOrder(35)]
    public bool RemoveMissingAfterImportDestinationTablesOnly { get; set; }

    [AddInParameter("Use strict primary key matching")]
    [AddInParameterEditor(typeof(YesNoParameterEditor), "Tooltip=This import affects ONLY records which match the selected primary key. If not checked the provider tries the following: Look at ProductID, Look at ProductNumber, Look at ProductName. If none match, create new record")]
    [AddInParameterGroup("Destination")]
    [AddInParameterOrder(38)]
    public bool UseStrictPrimaryKeyMatching { get; set; }

    [AddInParameter("Remove missing rows after import")]
    [AddInParameterEditor(typeof(YesNoParameterEditor), "Tooltip=Removes rows from the destination and relation tables. This option takes precedence")]
    [AddInParameterGroup("Destination")]
    [AddInParameterOrder(40)]
    public bool RemoveMissingAfterImport { get; set; }

    [AddInParameter("Update only existing products")]
    [AddInParameterEditor(typeof(YesNoParameterEditor), "")]
    [AddInParameterGroup("Destination")]
    [AddInParameterOrder(41)]
    public bool UpdateOnlyExistingProducts { get; set; }

    [AddInParameter("Create missing groups")]
    [AddInParameterEditor(typeof(YesNoParameterEditor), "")]
    [AddInParameterGroup("Destination")]
    [AddInParameterOrder(42)]
    public bool CreateMissingGoups { get; set; }

    [AddInParameter("Delete incoming rows")]
    [AddInParameterEditor(typeof(YesNoParameterEditor), "Tooltip=Deletes existing rows present in the import source. When Delete incoming rows is ON, the following options are skipped: Update only existing products, Update only existing records, Deactivate missing products, Remove missing rows after import, and Delete products / groups for languages included in input")]
    [AddInParameterGroup("Destination")]
    [AddInParameterOrder(50)]
    public bool DeleteIncomingItems { get; set; }

    [AddInParameter("Delete products/groups for languages included in input")]
    [AddInParameterEditor(typeof(YesNoParameterEditor), "Tooltip=Deletes products and groups only from the languages included in the import. When Delete incoming rows is ON, this option is ignored")]
    [AddInParameterGroup("Destination")]
    [AddInParameterOrder(60)]
    public bool DeleteProductsAndGroupForSpecificLanguage { get; set; }

    [AddInParameter("Discard duplicates")]
    [AddInParameterEditor(typeof(YesNoParameterEditor), "Tooltip=When ON, duplicate rows are skipped")]
    [AddInParameterGroup("Destination")]
    public bool DiscardDuplicates { get; set; }

    [AddInParameter("Hide deactivated products")]
    [AddInParameterEditor(typeof(YesNoParameterEditor), "Tooltip=When Deactivate missing products is ON, this option hides the deactivated products. If Delete incoming rows is ON, Hide deactivated products is skipped. If Deactivate missing products is OFF, Hide deactivated products is skipped")]
    [AddInParameterGroup("Destination")]
    [AddInParameterOrder(80)]
    public bool HideDeactivatedProducts { get; set; }

    [AddInParameter("User key field")]
    [AddInParameterEditor(typeof(TextParameterEditor), "")]
    [AddInParameterGroup("Hidden")]
    public string UserKeyField { get; set; } = "";

    [Obsolete("Use Job.RepositoriesIndexSettings")]
    public string RepositoriesIndexUpdate { get; set; } = "";

    [AddInParameter("Disable cache clearing")]
    [AddInParameterEditor(typeof(YesNoParameterEditor), "Tooltip=This setting disables cache clearing after import\t")]
    [AddInParameterGroup("Hidden")]
    [AddInParameterOrder(90)]
    public bool DisableCacheClearing { get; set; }

    [AddInParameter("Persist successful rows and skip failing rows")]
    [AddInParameterEditor(typeof(YesNoParameterEditor), "Tooltip=Checking this box allows the activity to do partial imports by skipping problematic records and keeping the succesful ones")]
    [AddInParameterGroup("Destination")]
    [AddInParameterOrder(100)]
    public bool SkipFailingRows { get; set; }

    [AddInParameter("Use existing Product Id found by Number in Variant Products")]
    [AddInParameterEditor(typeof(YesNoParameterEditor), "Tooltip=When checked, the values in Dynamicweb ProductId and ProductVariantID will be used at import to update products in Dynamicweb. A product is only updated if it matches the ProductNumber (in default language) of the imported row and if the Dynamicweb ProductVariantId field is not empty")]
    [AddInParameterGroup("Destination")]
    public bool UseProductIdFoundByNumber { get; set; }

    [AddInParameter("Ignore empty category field values")]
    [AddInParameterEditor(typeof(YesNoParameterEditor), "Tooltip=When checked, the Ecom provider does not write empty category field values to the database")]
    [AddInParameterGroup("Destination")]
    public bool IgnoreEmptyCategoryFieldValues { get; set; }
    #endregion

    /// <summary>
    /// This property is used to remove rows from the EcomGroupProductRelationsTable, but only for the products that are being imported.
    /// It can be set in the job settings xml file in the config section
    /// </summary>
    public virtual bool PartialUpdate { get; set; }

    private string? SqlConnectionString { get; set; }

    private SqlConnection? connection;
    protected SqlConnection Connection
    {
        get { return connection ??= (SqlConnection)Database.CreateConnection(); }
        set { connection = value; }
    }

    private EcomDestinationWriter? Writer;

    public EcomProvider(string connectionString)
    {
        RemoveMissingAfterImport = false;
        RemoveMissingAfterImportDestinationTablesOnly = false;
        UseStrictPrimaryKeyMatching = true;
        SqlConnectionString = connectionString;
        Connection = new SqlConnection(connectionString);
        DiscardDuplicates = false;
        HideDeactivatedProducts = false;
        CreateMissingGoups = true;
    }

    public override Schema GetOriginalDestinationSchema()
    {
        Schema result = GetOriginalSourceSchema();
        foreach (Table table in result.GetTables())
        {
            switch (table.Name)
            {
                case "EcomDiscount":
                    table.AddColumn(new SqlColumn("DiscountAccessUser", typeof(string), SqlDbType.NVarChar, table, -1, false, false, true));
                    table.AddColumn(new SqlColumn("DiscountAccessUserGroup", typeof(string), SqlDbType.NVarChar, table, -1, false, false, true));
                    break;
            }
        }
        return result;
    }

    private Schema GetSqlSchemas()
    {
        Schema result = GetDynamicwebSourceSchema();
        List<string> tablestToKeep = new()
        {
            "EcomProducts", "EcomManufacturers", "EcomGroups", "EcomVariantGroups", "EcomVariantsOptions",
            "EcomProductsRelated", "EcomProductItems", "EcomStockUnit", "EcomDetails","EcomProductCategoryFieldValue", "EcomLanguages", "EcomPrices",
            "EcomAssortmentGroupRelations", "EcomAssortmentPermissions", "EcomAssortmentProductRelations", "EcomAssortments", "EcomAssortmentShopRelations",
            "EcomVariantOptionsProductRelation", "EcomCurrencies", "EcomCountries", "EcomStockLocation", "EcomDiscount",
            "EcomUnits", "EcomUnitTranslations"
        };
        List<Table> tablesToRemove = new();
        foreach (Table table in result.GetTables())
        {
            if (!tablestToKeep.Contains(table.Name))
                tablesToRemove.Add(table);
        }
        foreach (Table table in tablesToRemove)
        {
            result.RemoveTable(table);
        }
        return result;
    }

    public override Schema GetOriginalSourceSchema()
    {
        Schema result = GetSqlSchemas();
        foreach (Table table in result.GetTables())
        {
            switch (table.Name)
            {
                case "EcomProducts":
                    //Add extra fields to EcomProducts
                    table.AddColumn(new SqlColumn(("Groups"), typeof(string), SqlDbType.NVarChar, table, -1, false, false, true));
                    table.AddColumn(new SqlColumn(("PrimaryGroup"), typeof(string), SqlDbType.NVarChar, table, -1, false, false, true));
                    table.AddColumn(new SqlColumn(("GroupSorting"), typeof(string), SqlDbType.NVarChar, table, -1, false, false, true));
                    table.AddColumn(new SqlColumn(("VariantGroups"), typeof(string), SqlDbType.NVarChar, table, -1, false, false, true));
                    table.AddColumn(new SqlColumn(("VariantOptions"), typeof(string), SqlDbType.NVarChar, table, -1, false, false, true));
                    table.AddColumn(new SqlColumn(("RelatedProducts"), typeof(string), SqlDbType.NVarChar, table, -1, false, false, true));
                    var fields = new List<Ecommerce.Products.Categories.Field>();
                    foreach (var category in Ecommerce.Services.ProductCategories.GetCategories())
                    {
                        if (category.CategoryType != Ecommerce.Products.Categories.CategoryType.SystemFields)
                        {
                            fields.AddRange(Dynamicweb.Ecommerce.Services.ProductCategoryFields.GetFieldsByCategoryId(category.Id));
                        }
                    }

                    foreach (var field in fields)
                    {
                        table.AddColumn(new SqlColumn(($"ProductCategory|{field.Category.Id}|{field.Id}"), typeof(string), SqlDbType.NVarChar, table, -1, false, false, true));
                    }
                    break;

                case "EcomGroups":
                    table.AddColumn(new SqlColumn("Shops", typeof(string), SqlDbType.NVarChar, table, -1, false, false, true));
                    table.AddColumn(new SqlColumn("ShopSorting", typeof(string), SqlDbType.NVarChar, table, -1, false, false, true));
                    table.AddColumn(new SqlColumn("ParentGroupsSorting", typeof(string), SqlDbType.NVarChar, table, -1, false, false, true));
                    table.AddColumn(new SqlColumn("ParentGroups", typeof(string), SqlDbType.NVarChar, table, -1, false, false, true));
                    break;
                case "EcomProductsRelated":
                    table.AddColumn(new SqlColumn("ProductRelatedLanguageID", typeof(string), SqlDbType.NVarChar, table, -1, false, false, true));
                    break;
                case "EcomProductCategoryFieldValue":
                    table.AddColumn(new SqlColumn("FieldValueProductNumber", typeof(string), SqlDbType.NVarChar, table, -1, false, false, true));
                    break;
                case "EcomStockUnit":
                    table.AddColumn(new SqlColumn("ProductName", typeof(string), SqlDbType.NVarChar, table, -1, false, false, true));
                    table.AddColumn(new SqlColumn("StockLocationName", typeof(string), SqlDbType.NVarChar, table, -1, false, false, true));
                    break;
            }
        }
        return result;
    }

    private Schema GetDynamicwebSourceSchema()
    {
        Schema result = GetSqlSourceSchema(Connection);
        var tables = result.GetTables();
        //set key for AccessUserTable
        if (UserKeyField != null)
        {
            UpdateColumn(tables?.Find(t => t.Name == "AccessUser"), UserKeyField);
        }

        //Set key for other tables that are missing keys in the database
        UpdateColumn(tables?.FirstOrDefault(t => t.Name == "Ecom7Tree"), "id");

        if (tables is not null && tables.Exists(t => t.Name.Contains("Ipaper")))
        {
            UpdateIPaperTables(result);
        }
        UpdateColumn(tables?.Find(t => t.Name == "Statv2SessionBot"), "Statv2SessionID");
        UpdateColumn(tables?.Find(t => t.Name == "Statv2UserAgents"), "Statv2UserAgentsID");

        //For EcomProducts Remove ProductAutoID column from schema
        Table? ecomProductsTable = tables?.Find(t => t.Name == "EcomProducts");
        if (ecomProductsTable != null)
        {
            ecomProductsTable.Columns.RemoveAll(c => c.Name == "ProductAutoID");
        }
        Table? ecomAssortmentPermissionsTable = tables?.Find(t => t.Name == "EcomAssortmentPermissions");
        if (ecomAssortmentPermissionsTable != null)
        {
            ecomAssortmentPermissionsTable.AddColumn(new SqlColumn(("AssortmentPermissionCustomerNumber"), typeof(string), SqlDbType.NVarChar, ecomAssortmentPermissionsTable, -1, false, false, true));
            ecomAssortmentPermissionsTable.AddColumn(new SqlColumn(("AssortmentPermissionExternalID"), typeof(string), SqlDbType.NVarChar, ecomAssortmentPermissionsTable, -1, false, false, true));
        }

        return result;
    }

    /// <summary>
    /// Gets source or destination schema for Ecom Provider
    /// </summary>
    /// <param name="getForDestination">true to get Destination schema, false to get Source schema</param>
    /// <returns></returns>
    [Obsolete("Use GetOriginalSourceSchema()")]
    public Schema GetSchema(bool getForDestination)
    {
        return GetOriginalSourceSchema();
    }

    private void UpdateIPaperTables(Schema schema)
    {
        UpdateColumn(schema.GetTables().Find(t => t.Name == "IpaperCategories"), "CategoryID");
        UpdateColumn(schema.GetTables().Find(t => t.Name == "IpaperLanguageKeys"), "LanguageKeyID");
        UpdateColumn(schema.GetTables().Find(t => t.Name == "IpaperLanguageKeyValues"), "LanguageKeyValueID");
        UpdateColumn(schema.GetTables().Find(t => t.Name == "IpaperLanguages"), "LanguageID");
        UpdateColumn(schema.GetTables().Find(t => t.Name == "IpaperPapers"), "PaperID");
        UpdateColumn(schema.GetTables().Find(t => t.Name == "IpaperSettingDescriptions"), "DescriptionID");
        UpdateColumn(schema.GetTables().Find(t => t.Name == "IpaperPages"), "PageID");
        UpdateColumn(schema.GetTables().Find(t => t.Name == "IpaperSettingGroups"), "GroupID");
        UpdateColumn(schema.GetTables().Find(t => t.Name == "IpaperSettings"), "SettingID");
        UpdateColumn(schema.GetTables().Find(t => t.Name == "IpaperSettingSets"), "SetID");
        UpdateColumn(schema.GetTables().Find(t => t.Name == "IpaperSettingTypes"), "TypeID");
    }

    private void UpdateColumn(Table? table, string columnName)
    {
        var column = table?.Columns?.Find(c => c.Name == columnName);
        if (column is not null)
        {
            column.IsPrimaryKey = true;
        }
    }

    public override void OverwriteSourceSchemaToOriginal()
    {
        Schema = GetOriginalSourceSchema();
    }

    public override void OverwriteDestinationSchemaToOriginal()
    {
        Schema = GetOriginalDestinationSchema();
    }

    Schema IDestination.GetSchema()
    {
        Schema ??= GetOriginalDestinationSchema();
        return Schema;
    }

    Schema ISource.GetSchema()
    {
        Schema ??= GetOriginalSourceSchema();
        return Schema;
    }

    public EcomProvider(XmlNode xmlNode)
    {
        RemoveMissingAfterImport = false;
        RemoveMissingAfterImportDestinationTablesOnly = false;
        UpdateOnlyExistingProducts = false;
        DeleteProductsAndGroupForSpecificLanguage = false;
        DeleteIncomingItems = false;
        DiscardDuplicates = false;
        HideDeactivatedProducts = false;
        InsertOnlyNewRecords = false;
        UseStrictPrimaryKeyMatching = true;
        CreateMissingGoups = true;

        foreach (XmlNode node in xmlNode.ChildNodes)
        {
            switch (node.Name)
            {
                case "SqlConnectionString":
                    if (node.HasChildNodes)
                    {
                        SqlConnectionString = node.FirstChild?.Value;
                        Connection = new SqlConnection(SqlConnectionString);
                    }
                    break;
                case "Schema":
                    Schema = new Schema(node);
                    break;
                case "RemoveMissingAfterImport":
                    if (node.HasChildNodes)
                    {
                        RemoveMissingAfterImport = node.FirstChild?.Value == "True";
                    }
                    break;
                case "RemoveMissingAfterImportDestinationTablesOnly":
                    if (node.HasChildNodes)
                    {
                        RemoveMissingAfterImportDestinationTablesOnly = node.FirstChild?.Value == "True";
                    }
                    break;
                case "DeactivateMissingProducts":
                    if (node.HasChildNodes)
                    {
                        DeactivateMissingProducts = node.FirstChild?.Value == "True";
                    }
                    break;
                case "Shop":
                    if (node.HasChildNodes)
                    {
                        Shop = node.FirstChild?.Value ?? "";
                    }
                    break;
                case "SourceShop":
                    if (node.HasChildNodes)
                    {
                        SourceShop = node.FirstChild?.Value ?? "";
                    }
                    break;
                case "UserKeyField":
                    if (node.HasChildNodes)
                    {
                        UserKeyField = node.FirstChild?.Value ?? "";
                    }
                    break;
                case "GroupsForProductsBy":
                    if (node.HasChildNodes)
                    {
                        GroupsForProductsBy = node.FirstChild?.Value ?? "";
                    }
                    break;
                case "GroupsForVariantOptionsBy":
                    if (node.HasChildNodes)
                    {
                        GroupsForVariantOptionsBy = node.FirstChild?.Value ?? "";
                    }
                    break;
                case "ManufacturerForProductsBy":
                    if (node.HasChildNodes)
                    {
                        ManufacturerForProductsBy = node.FirstChild?.Value ?? "";
                    }
                    break;
                case "RelatedProductGroupsBy":
                    if (node.HasChildNodes)
                    {
                        RelatedProductGroupsBy = node.FirstChild?.Value ?? "";
                    }
                    break;
                case "RelatedProductsBy":
                    if (node.HasChildNodes)
                    {
                        RelatedProductsBy = node.FirstChild?.Value ?? "";
                    }
                    break;
                case "VariantGroupsForProductsBy":
                    if (node.HasChildNodes)
                    {
                        VariantGroupsForProductsBy = node.FirstChild?.Value ?? "";
                    }
                    break;
                case "DefaultLanguage":
                    if (node.HasChildNodes)
                    {
                        DefaultLanguage = node.FirstChild?.Value ?? "";
                    }
                    break;
                case "SourceLanguage":
                    if (node.HasChildNodes)
                    {
                        SourceLanguage = node.FirstChild?.Value ?? "";
                    }
                    break;
                case "UpdateOnlyExistingProducts":
                    if (node.HasChildNodes)
                    {
                        UpdateOnlyExistingProducts = node.FirstChild?.Value == "True";
                    }
                    break;
                case "UseStrictPrimaryKeyMatching":
                    if (node.HasChildNodes)
                    {
                        UseStrictPrimaryKeyMatching = node.FirstChild?.Value == "True";
                    }
                    break;
                case "CreateMissingGoups":
                    if (node.HasChildNodes)
                    {
                        CreateMissingGoups = node.FirstChild?.Value == "True";
                    }
                    break;
                case "RepositoriesIndexUpdate":
                    if (node.HasChildNodes)
                    {
                        RepositoriesIndexUpdate = node.FirstChild?.Value ?? "";
                    }
                    break;
                case "DeleteProductsAndGroupForSpecificLanguage":
                    if (node.HasChildNodes)
                    {
                        DeleteProductsAndGroupForSpecificLanguage = node.FirstChild?.Value == "True";
                    }
                    break;
                case "UpdateOnlyExistingRecords":
                    if (node.HasChildNodes)
                    {
                        UpdateOnlyExistingRecords = node.FirstChild?.Value == "True";
                    }
                    break;
                case "DeleteIncomingItems":
                    if (node.HasChildNodes)
                    {
                        DeleteIncomingItems = node.FirstChild?.Value == "True";
                    }
                    break;
                case "DiscardDuplicates":
                    if (node.HasChildNodes)
                    {
                        DiscardDuplicates = node.FirstChild?.Value == "True";
                    }
                    break;
                case "HideDeactivatedProducts":
                    if (node.HasChildNodes)
                    {
                        HideDeactivatedProducts = node.FirstChild?.Value == "True";
                    }
                    break;
                case "InsertOnlyNewRecords":
                    if (node.HasChildNodes)
                    {
                        InsertOnlyNewRecords = node.FirstChild?.Value == "True";
                    }
                    break;
                case "SkipFailingRows":
                    if (node.HasChildNodes)
                    {
                        SkipFailingRows = node.FirstChild?.Value == "True";
                    }
                    break;
                case nameof(UseProductIdFoundByNumber):
                    if (node.HasChildNodes)
                    {
                        UseProductIdFoundByNumber = node.FirstChild?.Value == "True";
                    }
                    break;
                case nameof(IgnoreEmptyCategoryFieldValues):
                    if (node.HasChildNodes)
                    {
                        IgnoreEmptyCategoryFieldValues = node.FirstChild?.Value == "True";
                    }
                    break;
            }
        }
    }

    public override string ValidateDestinationSettings()
    {
        if (InsertOnlyNewRecords && UpdateOnlyExistingRecords)
        {
            return "\"Insert only\" and \"Update only\" options can not be set at the same time";
        }
        return "";
    }

    public override string ValidateSourceSettings()
    {
        return "";
    }

    void ISource.SaveAsXml(XmlTextWriter xmlTextWriter)
    {
        xmlTextWriter.WriteElementString("SqlConnectionString", SqlConnectionString);
        xmlTextWriter.WriteElementString("SourceShop", SourceShop);
        xmlTextWriter.WriteElementString("UserKeyField", UserKeyField);
        xmlTextWriter.WriteElementString("GroupsForProductsBy", GroupsForProductsBy);
        xmlTextWriter.WriteElementString("GroupsForVariantOptionsBy", GroupsForVariantOptionsBy);
        xmlTextWriter.WriteElementString("ManufacturerForProductsBy", ManufacturerForProductsBy);
        xmlTextWriter.WriteElementString("RelatedProductGroupsBy", RelatedProductGroupsBy);
        xmlTextWriter.WriteElementString("RelatedProductsBy", RelatedProductsBy);
        xmlTextWriter.WriteElementString("VariantGroupsForProductsBy", VariantGroupsForProductsBy);
        xmlTextWriter.WriteElementString("SourceLanguage", SourceLanguage);
        (this as ISource).GetSchema().SaveAsXml(xmlTextWriter);
    }

    void IDestination.SaveAsXml(XmlTextWriter xmlTextWriter)
    {
        xmlTextWriter.WriteElementString("RemoveMissingAfterImport", RemoveMissingAfterImport.ToString(CultureInfo.CurrentCulture));
        xmlTextWriter.WriteElementString("RemoveMissingAfterImportDestinationTablesOnly", RemoveMissingAfterImportDestinationTablesOnly.ToString(CultureInfo.CurrentCulture));
        xmlTextWriter.WriteElementString("DeactivateMissingProducts", DeactivateMissingProducts.ToString(CultureInfo.CurrentCulture));
        xmlTextWriter.WriteElementString("DeleteProductsAndGroupForSpecificLanguage", DeleteProductsAndGroupForSpecificLanguage.ToString(CultureInfo.CurrentCulture));
        xmlTextWriter.WriteElementString("Shop", Shop);
        xmlTextWriter.WriteElementString("DefaultLanguage", DefaultLanguage);
        xmlTextWriter.WriteElementString("UpdateOnlyExistingProducts", UpdateOnlyExistingProducts.ToString(CultureInfo.CurrentCulture));
        xmlTextWriter.WriteElementString("UseStrictPrimaryKeyMatching", UseStrictPrimaryKeyMatching.ToString(CultureInfo.CurrentCulture));
        xmlTextWriter.WriteElementString("RepositoriesIndexUpdate", RepositoriesIndexUpdate);
        xmlTextWriter.WriteElementString("UpdateOnlyExistingRecords", UpdateOnlyExistingRecords.ToString(CultureInfo.CurrentCulture));
        xmlTextWriter.WriteElementString("DeleteIncomingItems", DeleteIncomingItems.ToString(CultureInfo.CurrentCulture));
        xmlTextWriter.WriteElementString("DiscardDuplicates", DiscardDuplicates.ToString(CultureInfo.CurrentCulture));
        xmlTextWriter.WriteElementString("HideDeactivatedProducts", HideDeactivatedProducts.ToString(CultureInfo.CurrentCulture));
        xmlTextWriter.WriteElementString("InsertOnlyNewRecords", InsertOnlyNewRecords.ToString(CultureInfo.CurrentCulture));
        xmlTextWriter.WriteElementString("CreateMissingGoups", CreateMissingGoups.ToString(CultureInfo.CurrentCulture));
        xmlTextWriter.WriteElementString(nameof(UseProductIdFoundByNumber), UseProductIdFoundByNumber.ToString());
        xmlTextWriter.WriteElementString(nameof(IgnoreEmptyCategoryFieldValues), IgnoreEmptyCategoryFieldValues.ToString());
        xmlTextWriter.WriteElementString("SkipFailingRows", SkipFailingRows.ToString(CultureInfo.CurrentCulture));
        (this as IDestination).GetSchema().SaveAsXml(xmlTextWriter);
    }

    public override void UpdateSourceSettings(ISource source)
    {
        EcomProvider newProvider = (EcomProvider)source;
        SourceLanguage = newProvider.SourceLanguage;
        SourceShop = newProvider.SourceShop;
        GetGroupNamesForProduct = newProvider.GetGroupNamesForProduct;
        GetGroupNamesForVariantOptions = newProvider.GetGroupNamesForVariantOptions;
        GetManufacturerNamesForProducts = newProvider.GetManufacturerNamesForProducts;
        GetRelatedProductGroupsByName = newProvider.GetRelatedProductGroupsByName;
        GetRelatedProductsByName = newProvider.GetRelatedProductsByName;
        GetVariantGroupNamesForProduct = newProvider.GetVariantGroupNamesForProduct;
        SqlConnectionString = newProvider.SqlConnectionString;
    }

    public override void UpdateDestinationSettings(IDestination destination)
    {
        EcomProvider newProvider = (EcomProvider)destination;
        DefaultLanguage = newProvider.DefaultLanguage;
        Shop = newProvider.Shop;
        DeactivateMissingProducts = newProvider.DeactivateMissingProducts;
        UpdateOnlyExistingProducts = newProvider.UpdateOnlyExistingProducts;
        UseStrictPrimaryKeyMatching = newProvider.UseStrictPrimaryKeyMatching;
        DeleteProductsAndGroupForSpecificLanguage = newProvider.DeleteProductsAndGroupForSpecificLanguage;
        UpdateOnlyExistingRecords = newProvider.UpdateOnlyExistingRecords;
        DeleteIncomingItems = newProvider.DeleteIncomingItems;
        DiscardDuplicates = newProvider.DiscardDuplicates;
        HideDeactivatedProducts = newProvider.HideDeactivatedProducts;
        InsertOnlyNewRecords = newProvider.InsertOnlyNewRecords;
        CreateMissingGoups = newProvider.CreateMissingGoups;
        SkipFailingRows = newProvider.SkipFailingRows;
        UseProductIdFoundByNumber = newProvider.UseProductIdFoundByNumber;
        IgnoreEmptyCategoryFieldValues = newProvider.IgnoreEmptyCategoryFieldValues;
        RemoveMissingAfterImport = newProvider.RemoveMissingAfterImport;
        RemoveMissingAfterImportDestinationTablesOnly = newProvider.RemoveMissingAfterImportDestinationTablesOnly;
    }

    public override string Serialize()
    {
        XDocument document = new XDocument(new XDeclaration("1.0", "utf-8", string.Empty));
        XElement root = new XElement("Parameters");
        document.Add(root);
        root.Add(CreateParameterNode(GetType(), "Get groups for variant options by:", GroupsForVariantOptionsBy));
        root.Add(CreateParameterNode(GetType(), "Get manufacturer for products by:", ManufacturerForProductsBy));
        root.Add(CreateParameterNode(GetType(), "Get variant groups for products by:", VariantGroupsForProductsBy));
        root.Add(CreateParameterNode(GetType(), "Get groups for products by:", GroupsForProductsBy));
        root.Add(CreateParameterNode(GetType(), "Get related products by:", RelatedProductsBy));
        root.Add(CreateParameterNode(GetType(), "Get related product groups by:", RelatedProductGroupsBy));
        root.Add(CreateParameterNode(GetType(), "Default Language", DefaultLanguage));
        root.Add(CreateParameterNode(GetType(), "Source language", SourceLanguage ?? ""));
        root.Add(CreateParameterNode(GetType(), "Deactivate missing products", DeactivateMissingProducts.ToString()));
        root.Add(CreateParameterNode(GetType(), "Delete products/groups for languages included in input", DeleteProductsAndGroupForSpecificLanguage.ToString()));
        root.Add(CreateParameterNode(GetType(), "Shop", Shop ?? ""));
        root.Add(CreateParameterNode(GetType(), "Source shop", SourceShop ?? ""));
        root.Add(CreateParameterNode(GetType(), "User key field", UserKeyField ?? ""));
        root.Add(CreateParameterNode(GetType(), "Remove missing rows after import", RemoveMissingAfterImport.ToString()));
        root.Add(CreateParameterNode(GetType(), "Remove missing rows after import in the destination tables only", RemoveMissingAfterImportDestinationTablesOnly.ToString()));
        root.Add(CreateParameterNode(GetType(), "Update only existing products", UpdateOnlyExistingProducts.ToString()));
        root.Add(CreateParameterNode(GetType(), "Use strict primary key matching", UseStrictPrimaryKeyMatching.ToString()));
        root.Add(CreateParameterNode(GetType(), "Update only existing records", UpdateOnlyExistingRecords.ToString()));
        root.Add(CreateParameterNode(GetType(), "Delete incoming rows", DeleteIncomingItems.ToString()));
        root.Add(CreateParameterNode(GetType(), "Discard duplicates", DiscardDuplicates.ToString()));
        root.Add(CreateParameterNode(GetType(), "Hide deactivated products", HideDeactivatedProducts.ToString()));
        root.Add(CreateParameterNode(GetType(), "Insert only new records", InsertOnlyNewRecords.ToString()));
        root.Add(CreateParameterNode(GetType(), "Create missing groups", CreateMissingGoups.ToString()));
        root.Add(CreateParameterNode(GetType(), "Persist successful rows and skip failing rows", SkipFailingRows.ToString()));
        root.Add(CreateParameterNode(GetType(), "Use existing Product Id found by Number in Variant Products", UseProductIdFoundByNumber.ToString()));
        root.Add(CreateParameterNode(GetType(), "Ignore empty category field values", IgnoreEmptyCategoryFieldValues.ToString()));

        return document.ToString();
    }

    public EcomProvider()
    {
        UseStrictPrimaryKeyMatching = true;
        CreateMissingGoups = true;
    }

    public override ISourceReader GetReader(Mapping mapping)
    {
        return new EcomSourceReader(mapping, Connection, GetGroupNamesForVariantOptions, GetManufacturerNamesForProducts, GetGroupNamesForProduct, GetVariantGroupNamesForProduct, GetRelatedProductsByName, GetRelatedProductGroupsByName, SourceLanguage, SourceShop);
    }

    public override void OrderTablesInJob(Job job, bool isSource)
    {
        MappingCollection tables = new MappingCollection();

        var mappings = GetMappingsByName(job.Mappings, "EcomLanguages", isSource);
        if (mappings != null)
            tables.AddRange(mappings);

        mappings = GetMappingsByName(job.Mappings, "EcomCountries", isSource);
        if (mappings != null)
            tables.AddRange(mappings);

        mappings = GetMappingsByName(job.Mappings, "EcomCurrencies", isSource);
        if (mappings != null)
            tables.AddRange(mappings);

        mappings = GetMappingsByName(job.Mappings, "EcomStockLocation", isSource);
        if (mappings != null)
            tables.AddRange(mappings);

        mappings = GetMappingsByName(job.Mappings, "EcomGroups", isSource);
        if (mappings != null)
            tables.AddRange(mappings);

        mappings = GetMappingsByName(job.Mappings, "EcomManufacturers", isSource);
        if (mappings != null)
            tables.AddRange(mappings);

        mappings = GetMappingsByName(job.Mappings, "EcomVariantGroups", isSource);
        if (mappings != null)
            tables.AddRange(mappings);

        mappings = GetMappingsByName(job.Mappings, "EcomVariantsOptions", isSource);
        if (mappings != null)
            tables.AddRange(mappings);

        mappings = GetMappingsByName(job.Mappings, "EcomProducts", isSource);
        if (mappings != null)
            tables.AddRange(mappings);

        mappings = GetMappingsByName(job.Mappings, "EcomProductItems", isSource);
        if (mappings != null)
            tables.AddRange(mappings);

        mappings = GetMappingsByName(job.Mappings, "EcomProductsRelated", isSource);
        if (mappings != null)
            tables.AddRange(mappings);

        mappings = GetMappingsByName(job.Mappings, "EcomStockUnit", isSource);
        if (mappings != null)
            tables.AddRange(mappings);

        mappings = GetMappingsByName(job.Mappings, "EcomDetails", isSource);
        if (mappings != null)
            tables.AddRange(mappings);

        mappings = GetMappingsByName(job.Mappings, "EcomProductCategoryFieldValue", isSource);
        if (mappings != null)
            tables.AddRange(mappings);

        mappings = GetMappingsByName(job.Mappings, "EcomPrices", isSource);
        if (mappings != null)
            tables.AddRange(mappings);

        mappings = GetMappingsByName(job.Mappings, "EcomDiscount", isSource);
        if (mappings != null)
            tables.AddRange(mappings);

        mappings = GetMappingsByName(job.Mappings, "EcomAssortments", isSource);
        if (mappings != null)
            tables.AddRange(mappings);

        mappings = GetMappingsByName(job.Mappings, "EcomAssortmentPermissions", isSource);
        if (mappings != null)
            tables.AddRange(mappings);

        mappings = GetMappingsByName(job.Mappings, "EcomAssortmentGroupRelations", isSource);
        if (mappings != null)
            tables.AddRange(mappings);

        mappings = GetMappingsByName(job.Mappings, "EcomAssortmentProductRelations", isSource);
        if (mappings != null)
            tables.AddRange(mappings);

        mappings = GetMappingsByName(job.Mappings, "EcomAssortmentShopRelations", isSource);
        if (mappings != null)
            tables.AddRange(mappings);

        mappings = GetMappingsByName(job.Mappings, "EcomVariantOptionsProductRelation", isSource);
        if (mappings != null)
            tables.AddRange(mappings);

        mappings = GetMappingsByName(job.Mappings, "EcomUnits", isSource);
        if (mappings != null)
            tables.AddRange(mappings);

        mappings = GetMappingsByName(job.Mappings, "EcomUnitTranslations", isSource);
        if (mappings != null)
            tables.AddRange(mappings);

        job.Mappings = tables;
    }

    internal static IEnumerable<Mapping> GetMappingsByName(MappingCollection collection, string name, bool isSource)
    {
        if (isSource)
        {
            return collection.FindAll(map => map.SourceTable != null && map.SourceTable.Name == name);
        }
        else
        {
            return collection.FindAll(map => map.DestinationTable != null && map.DestinationTable.Name == name);
        }
    }

    public override bool RunJob(Job job)
    {
        ReplaceMappingConditionalsWithValuesFromRequest(job);
        if (IsFirstJobRun)
        {
            OrderTablesInJob(job, false);
        }
        SqlTransaction? sqlTransaction = null;
        if (Connection.State.ToString() != "Open")
            Connection.Open();

        Dictionary<string, object>? sourceRow = null;
        Exception? exception = null;

        try
        {
            if (IsFirstJobRun)
            {
                Writer = new EcomDestinationWriter(job, Connection, DeactivateMissingProducts, null, RemoveMissingAfterImport, Logger,
                UpdateOnlyExistingProducts, DefaultLanguage, DiscardDuplicates, PartialUpdate, RemoveMissingAfterImportDestinationTablesOnly, UseStrictPrimaryKeyMatching,
                CreateMissingGoups, SkipFailingRows, UseProductIdFoundByNumber, IgnoreEmptyCategoryFieldValues);
                if (!string.IsNullOrEmpty(Shop))
                {
                    Writer.DefaultShop = Shop;
                }
            }
            else
            {
                if (Writer == null)
                {
                    throw new Exception($"Can not find Ecom");
                }
            }

            foreach (Mapping mapping in job.Mappings)
            {
                var columnMappings = mapping.GetColumnMappings();

                if (mapping.Active && columnMappings.Count > 0)
                {
                    if (!string.IsNullOrEmpty(Shop))
                    {
                        string destinationColumnNameForShopId = MappingExtensions.GetShopIdColumnName(mapping.DestinationTable.Name);
                        if (!string.IsNullOrEmpty(destinationColumnNameForShopId) && !columnMappings.Any(obj => obj.Active && obj.DestinationColumn.Name.Equals(destinationColumnNameForShopId, StringComparison.OrdinalIgnoreCase)))
                        {
                            Column randomColumn = mapping.SourceTable.Columns.First();
                            var shopColumnMapping = mapping.AddMapping(randomColumn, mapping.DestinationTable.Columns.Find(c => string.Compare(c.Name, MappingExtensions.GetShopIdColumnName(mapping.DestinationTable.Name), true) == 0));
                            shopColumnMapping.ScriptType = ScriptType.Constant;
                            shopColumnMapping.ScriptValue = Shop;
                        }
                    }

                    Logger.Log("Starting import to temporary table for " + mapping.DestinationTable.Name + ".");
                    using (var reader = job.Source.GetReader(mapping))
                    {
                        bool? optionValue = mapping.GetOptionValue("DiscardDuplicates");
                        bool discardDuplicates = optionValue.HasValue ? optionValue.Value : DiscardDuplicates;

                        while (!reader.IsDone())
                        {
                            sourceRow = reader.GetNext();
                            if (ProcessInputRow(sourceRow, mapping))
                            {
                                Writer.Write(sourceRow, mapping, discardDuplicates);
                            }
                        }
                        Writer.ReportProgress(mapping);
                    }
                    if (mapping.DestinationTable.Name == "EcomProducts" && !CreateMissingGoups)
                    {
                        Writer.FailOnMissingGroups();
                    }
                    Logger.Log("Finished import to temporary table for " + mapping.DestinationTable.Name + ".");
                }
            }
            sourceRow = null;

            Logger.Log("Starting update products information.");
            Writer.UpdateProductRelatedProducts();
            Writer.UpdateVariantFieldsInProducts();
            Writer.UpdateFieldsInExistingProductsWithVariantIDs();
            Writer.UpdateGroupRelations();
            Logger.Log("Update products information finished.");
            Writer.FinishWriting();
            sqlTransaction = Connection.BeginTransaction();
            if (DeleteIncomingItems)
            {
                Writer.DeleteExistingFromMainTable(Shop, sqlTransaction, DefaultLanguage);
            }
            else
            {
                Writer.MoveDataToMainTables(Shop, sqlTransaction, UpdateOnlyExistingRecords, InsertOnlyNewRecords);
                Writer.DeleteExcessFromMainTable(Shop, sqlTransaction, DefaultLanguage, DeleteProductsAndGroupForSpecificLanguage, HideDeactivatedProducts);
            }
            Writer.CleanRelationsTables(sqlTransaction);
            sqlTransaction.Commit();
            Writer.RebuildAssortments();
            
            MoveRepositoriesIndexToJob(job);
        }
        catch (Exception ex)
        {
            exception = ex;
            string msg = ex.Message;
            string? stackTrace = ex.StackTrace;

            Logger?.Error($"Error: {msg.Replace(System.Environment.NewLine, " ")} Stack: {stackTrace?.Replace(System.Environment.NewLine, " ")}", ex);
            LogManager.System.GetLogger(LogCategory.Application, "Dataintegration").Error($"{GetType().Name} error: {msg} Stack: {stackTrace}", ex);

            if (ex.Message.Contains("Subquery returned more than 1 value"))
                msg += System.Environment.NewLine + "When using Ecom Provider and no ProductID is given, product is next recognized on ProductNumber. This error usually indicates duplicates on column ProductNumber.";

            if (ex.Message.Contains("Bulk copy failures"))
            {
                Logger?.Log("Job Failed with the following message:");
                BulkCopyHelper.LogFailedRows(Logger, msg);
            }
            else if (ex.Message.Contains(EcomDestinationWriter.EcomProductsMissingGroupsErrorMessage) && Writer != null)
            {
                Logger?.Log("Job Failed with the following message:");
                Writer.LogFailedRows();
            }
            else
            {
                if (sourceRow != null)
                    msg += GetFailedSourceRowMessage(sourceRow);
                Logger?.Log("Job Failed with the following message: " + msg);
            }

            if (sqlTransaction != null)
                sqlTransaction.Rollback();
            return false;
        }
        finally
        {
            Writer?.Close();
        }
        if (IsFirstJobRun)
        {
            IsFirstJobRun = false;
        }
        return true;
    }

    private void MoveRepositoriesIndexToJob(Job job)
    {
        if (!string.IsNullOrEmpty(RepositoriesIndexUpdate))
        {
            char[] separator = [','];
            // if the provider already have RepositoriesIndexUpdate set, then we move them to the job, and set the add-in to string.empty
            if (job.RepositoriesIndexSettings?.RepositoriesIndexes?.Count == 0)
            {
                job.RepositoriesIndexSettings = new RepositoriesIndexSettings(new Collection<string>([.. RepositoriesIndexUpdate.Split(separator, StringSplitOptions.RemoveEmptyEntries)]));
            }
            RepositoriesIndexUpdate = string.Empty;
            job.Save();
        }
    }

    public override void Close()
    {
        Connection.Close();
    }

    public IEnumerable<ParameterOption> GetParameterOptions(string parameterName)
    {
        return parameterName switch
        {
            "Default Language" => GetDefaultLanguageOptions(),
            "Source language" => GetDefaultLanguageOptions(),
            "Shop" => GetShopOptions(),
            "Source shop" => GetShopOptions(),
            "Product index update" => new List<ParameterOption>()
                {
                    new("Full", "Full"),
                    new("Partial", "Partial")
                },
            _ => new List<ParameterOption>()
                {
                    new("Name", "Name"),
                    new("ID", "ID")
                },
        };
    }

    private IEnumerable<ParameterOption> GetShopOptions()
    {
        var options = new List<ParameterOption>();
        var sqlCommand = GetOpenConnection();
        var languagesDataAdapter = new SqlDataAdapter("SELECT ShopID, ShopName FROM EcomShops", sqlCommand.Connection);
        _ = new SqlCommandBuilder(languagesDataAdapter);
        var dataSet = new DataSet();
        languagesDataAdapter.Fill(dataSet);
        foreach (DataRow row in dataSet.Tables[0].Rows)
        {
            options.Add(new(row["ShopName"].ToString(), row["shopID"]));
        }
        return options;
    }

    private IEnumerable<ParameterOption> GetDefaultLanguageOptions()
    {
        var options = new List<ParameterOption>();
        var sqlCommand = GetOpenConnection();
        var languagesDataAdapter = new SqlDataAdapter("SELECT LanguageID, LanguageCode2, LanguageName FROM EcomLanguages", sqlCommand.Connection);
        _ = new SqlCommandBuilder(languagesDataAdapter);
        var languageDataSet = new DataSet();
        languagesDataAdapter.Fill(languageDataSet);
        foreach (DataRow row in languageDataSet.Tables[0].Rows)
        {
            options.Add(new(row["LanguageName"].ToString(), row["LanguageID"]));
        }
        return options;
    }

    private SqlCommand GetOpenConnection()
    {
        SqlCommand sqlCommand = new() { Connection = Connection };
        if (Connection.State == ConnectionState.Closed)
            Connection.Open();
        return sqlCommand;
    }


    IEnumerable<string> IParameterVisibility.GetHiddenParameterNames(string parameterName, object parameterValue)
    {
        var result = new List<string>();
        switch (parameterName)
        {
            case "Default Language":
                if (string.IsNullOrEmpty(defaultLanguage) || defaultLanguage.Equals(Ecommerce.Services.Languages.GetDefaultLanguageId(), StringComparison.OrdinalIgnoreCase))
                    result.Add("Default Language");
                break;
            case "Source language":
                if (string.IsNullOrEmpty(SourceLanguage))
                    result.Add("Source language");
                break;
            case "Shop":
                if (string.IsNullOrEmpty(Shop))
                    result.Add("Shop");
                break;
            case "Source shop":
                if (string.IsNullOrEmpty(SourceShop))
                    result.Add("Source shop");
                break;
        }
        return result;
    }
}

