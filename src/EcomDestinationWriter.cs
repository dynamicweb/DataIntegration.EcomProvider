﻿using Dynamicweb.Configuration;
using Dynamicweb.Core;
using Dynamicweb.Data;
using Dynamicweb.DataIntegration.Integration;
using Dynamicweb.DataIntegration.ProviderHelpers;
using Dynamicweb.Ecommerce.Stocks;
using Dynamicweb.Logging;
using System;
using System.Collections;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using System.Linq;
using System.Text;

namespace Dynamicweb.DataIntegration.Providers.EcomProvider;

internal class EcomDestinationWriter : BaseSqlWriter
{
    private readonly Job job;
    private readonly SqlConnection connection;
    private readonly SqlCommand sqlCommand;
    private readonly bool deactivateMissingProducts;
    private readonly bool updateOnlyExistingProducts;
    protected internal DataSet DataToWrite = new DataSet();
    private readonly ILogger logger;
    //Used for fast searching of VariantOptionsProductRelation combinations
    private Hashtable ecomVariantOptionsProductRelationKeys = new Hashtable();
    //Used for fast searching of GroupProductRelation combinations
    private Hashtable ecomGroupProductRelationKeys = new Hashtable();
    //Used for fast searching of VariantgroupProductrelation combinations
    private Hashtable ecomVariantgroupProductrelationKeys = new Hashtable();
    private AssortmentHandler? assortmentHandler = null;
    private bool isParentGroupSortingInEcomGroupsMapping = false;
    protected DuplicateRowsHandler? duplicateRowsHandler;
    private Hashtable _processedProductsKeys = new Hashtable();
    private SortedList<string, int> ProductVariantsCountDictionary = new SortedList<string, int>();
    private SortedList<string, int> ProductVariantGroupsCountDictionary = new SortedList<string, int>();
    private readonly bool partialUpdate;
    private readonly Dictionary<int, IEnumerable<string>> MappingIdEcomProductsPKColumns;
    private readonly bool removeMissingAfterImportDestinationTablesOnly;
    private readonly bool useStrictPrimaryKeyMatching;
    private readonly bool _createMissingGoups;
    private readonly bool _skipFailingRows;
    private readonly bool _useProductIdFoundByNumber;
    private readonly bool _ignoreEmptyCategoryFieldValues;
    private List<Dictionary<string, object>> _rowsWithMissingGroups = new List<Dictionary<string, object>>();
    internal const string EcomProductsMissingGroupsErrorMessage = "Failed at importing EcomProducts rows with missing Groups";

    private Dictionary<string, List<Mapping>> Mappings = new Dictionary<string, List<Mapping>>(StringComparer.OrdinalIgnoreCase);
    private Dictionary<string, Dictionary<string, Column>> DestinationColumnMappings = new Dictionary<string, Dictionary<string, Column>>(StringComparer.OrdinalIgnoreCase);
    private Dictionary<int, Dictionary<string, ColumnMapping>> SourceColumnMappings = new Dictionary<int, Dictionary<string, ColumnMapping>>();

    private Dictionary<int, ColumnMappingCollection> ColumnMappingsByMappingId = new Dictionary<int, ColumnMappingCollection>();

    private long RowAutoId = 0;
    protected internal Dictionary<string, Dictionary<string, List<DataRow>>> DataRowsToWrite = new Dictionary<string, Dictionary<string, List<DataRow>>>(StringComparer.OrdinalIgnoreCase);
    private Dictionary<string, DataRow> ImportedProductsByNumber = new Dictionary<string, DataRow>(StringComparer.OrdinalIgnoreCase);
    private List<Mapping> _addedMappingsForMoveToMainTables = new List<Mapping>();
    internal const string TempTableName = "TempTableForBulkImport";

    internal int RowsToWriteCount
    {
        get;
        private set;
    }

    public EcomDestinationWriter(Job job, SqlConnection connection, bool deactivateMissinProducts, SqlCommand? commandForTest, bool deleteExcess, ILogger logger,
        bool updateOnlyExistingProducts, string defaultLanguageId, bool discardDuplicates, bool partialUpdate)
        : this(job, connection, deactivateMissinProducts, commandForTest, deleteExcess, logger,
        updateOnlyExistingProducts, defaultLanguageId, discardDuplicates, partialUpdate, false)
    {
    }

    public EcomDestinationWriter(Job job, SqlConnection connection, bool deactivateMissinProducts, SqlCommand? commandForTest, bool deleteExcess, ILogger logger,
    bool updateOnlyExistingProducts, string defaultLanguageId, bool discardDuplicates, bool partialUpdate, bool removeMissingAfterImportDestinationTablesOnly)
       : this(job, connection, deactivateMissinProducts, commandForTest, deleteExcess, logger,
        updateOnlyExistingProducts, defaultLanguageId, discardDuplicates, partialUpdate, removeMissingAfterImportDestinationTablesOnly, false, true)
    {
    }

    public EcomDestinationWriter(Job job, SqlConnection connection, bool deactivateMissinProducts, SqlCommand? commandForTest, bool deleteExcess, ILogger logger,
    bool updateOnlyExistingProducts, string defaultLanguageId, bool discardDuplicates, bool partialUpdate, bool removeMissingAfterImportDestinationTablesOnly,
    bool useStrictPrimaryKeyMatching, bool createMissingGoups)
        : this(job, connection, deactivateMissinProducts, commandForTest, deleteExcess, logger,
             updateOnlyExistingProducts, defaultLanguageId, discardDuplicates, partialUpdate, removeMissingAfterImportDestinationTablesOnly, useStrictPrimaryKeyMatching, createMissingGoups, false)
    {
    }

    public EcomDestinationWriter(Job job, SqlConnection connection, bool deactivateMissinProducts, SqlCommand? commandForTest, bool deleteExcess, ILogger logger,
    bool updateOnlyExistingProducts, string defaultLanguageId, bool discardDuplicates, bool partialUpdate, bool removeMissingAfterImportDestinationTablesOnly,
    bool useStrictPrimaryKeyMatching, bool createMissingGoups, bool skipFailingRows)
        : this(job, connection, deactivateMissinProducts, commandForTest, deleteExcess, logger,
             updateOnlyExistingProducts, defaultLanguageId, discardDuplicates, partialUpdate, removeMissingAfterImportDestinationTablesOnly, useStrictPrimaryKeyMatching, createMissingGoups, skipFailingRows, false)
    {
    }
    public EcomDestinationWriter(Job job, SqlConnection connection, bool deactivateMissinProducts, SqlCommand? commandForTest, bool deleteExcess, ILogger logger,
    bool updateOnlyExistingProducts, string defaultLanguageId, bool discardDuplicates, bool partialUpdate, bool removeMissingAfterImportDestinationTablesOnly,
    bool useStrictPrimaryKeyMatching, bool createMissingGoups, bool skipFailingRows, bool useProductIdFoundByNumber)
        : this(job, connection, deactivateMissinProducts, commandForTest, deleteExcess, logger,
             updateOnlyExistingProducts, defaultLanguageId, discardDuplicates, partialUpdate, removeMissingAfterImportDestinationTablesOnly, useStrictPrimaryKeyMatching, createMissingGoups, skipFailingRows, useProductIdFoundByNumber, false)
    {
    }

    public EcomDestinationWriter(Job job, SqlConnection connection, bool deactivateMissinProducts, SqlCommand? commandForTest, bool deleteExcess, ILogger logger,
    bool updateOnlyExistingProducts, string defaultLanguageId, bool discardDuplicates, bool partialUpdate, bool removeMissingAfterImportDestinationTablesOnly,
    bool useStrictPrimaryKeyMatching, bool createMissingGoups, bool skipFailingRows, bool useProductIdFoundByNumber, bool ignoreEmptyCategoryFieldValues)
    {
        Ensure.NotNull(job);
        deactivateMissingProducts = deactivateMissinProducts;
        this.updateOnlyExistingProducts = updateOnlyExistingProducts;
        this.deleteExcess = deleteExcess;
        this.job = job;
        bool removeMissing = ((EcomProvider)this.job.Destination).RemoveMissingAfterImport || ((EcomProvider)this.job.Destination).RemoveMissingAfterImportDestinationTablesOnly;
        _removeFromEcomGroups = removeMissing;
        _removeFromEcomVariantGroups = removeMissing;
        this.connection = connection;
        this.logger = logger;
        if (commandForTest == null)
        {
            sqlCommand = connection.CreateCommand();
        }
        else
        {
            sqlCommand = commandForTest;
        }
        InitMappings();
        CreateTempTables();

        if (string.IsNullOrEmpty(defaultLanguageId))
        {
            _defaultLanguageId = Ecommerce.Services.Languages.GetDefaultLanguageId();
            if (string.IsNullOrEmpty(_defaultLanguageId))
            {
                _defaultLanguageId = "LANG1";
            }
        }
        else
        {
            _defaultLanguageId = defaultLanguageId;
        }
        assortmentHandler = new AssortmentHandler(sqlCommand, this.logger);
        if (this.job.Mappings != null)
        {
            if (DestinationColumnMappings.TryGetValue("EcomGroups", out Dictionary<string, Column>? ecomGroupsMapping) && ecomGroupsMapping.ContainsKey("ParentGroupsSorting"))
            {
                isParentGroupSortingInEcomGroupsMapping = true;
            }
        }
        bool discardDuplicatesFromMapping = false;
        if (!discardDuplicates)
        {
            discardDuplicatesFromMapping = job.Mappings.Where(m => m != null && m.Active).Any(m =>
            {
                bool? v = m.GetOptionValue("DiscardDuplicates");
                return v.HasValue && v.Value;
            });
        }
        if (discardDuplicates || discardDuplicatesFromMapping)
        {
            duplicateRowsHandler = new DuplicateRowsHandler(logger, job.Mappings);
        }
        EnsureConnectionIsOpen();

        this.partialUpdate = partialUpdate;
        MappingIdEcomProductsPKColumns = GetEcomProductsPKColumns();
        this.removeMissingAfterImportDestinationTablesOnly = removeMissingAfterImportDestinationTablesOnly;
        this.useStrictPrimaryKeyMatching = useStrictPrimaryKeyMatching;
        _createMissingGoups = createMissingGoups;
        _skipFailingRows = skipFailingRows;
        if (useProductIdFoundByNumber && DestinationColumnMappings.TryGetValue("EcomProducts", out Dictionary<string, Column>? mapping)
            && mapping.ContainsKey("ProductNumber") && mapping.ContainsKey("ProductVariantID"))
        {
            _useProductIdFoundByNumber = true;
        }
        _ignoreEmptyCategoryFieldValues = ignoreEmptyCategoryFieldValues;
    }

    private void InitMappings()
    {
        foreach (var mapping in job.Mappings)
        {
            var mappingColumns = mapping.GetColumnMappings();
            ColumnMappingsByMappingId.Add(mapping.GetId(), mappingColumns);
            InitMappingsByTableName(mapping);
            InitColumnMappings(mapping, mappingColumns);
        }
    }

    private void InitMappingsByTableName(Mapping mapping)
    {
        List<Mapping>? mappingsList = null;
        if (!Mappings.TryGetValue(mapping.DestinationTable.Name, out mappingsList))
        {
            mappingsList = new List<Mapping>();
            Mappings.Add(mapping.DestinationTable.Name, mappingsList);
        }
        mappingsList.Add(mapping);
    }

    private void InitColumnMappings(Mapping mapping, ColumnMappingCollection mappings)
    {
        string tableName = mapping.DestinationTable.Name;
        Dictionary<string, Column>? destinationColumnMappingDictionary;
        Dictionary<string, ColumnMapping>? sourceColumnMappingDictionary;
        if (!SourceColumnMappings.TryGetValue(mapping.GetId(), out sourceColumnMappingDictionary))
        {
            sourceColumnMappingDictionary = new Dictionary<string, ColumnMapping>(StringComparer.OrdinalIgnoreCase);
            SourceColumnMappings.Add(mapping.GetId(), sourceColumnMappingDictionary);
        }

        if (!DestinationColumnMappings.TryGetValue(tableName, out destinationColumnMappingDictionary))
        {
            destinationColumnMappingDictionary = new Dictionary<string, Column>(StringComparer.OrdinalIgnoreCase);
            DestinationColumnMappings.Add(tableName, destinationColumnMappingDictionary);
        }
        foreach (var columnMapping in mappings)
        {
            if (!destinationColumnMappingDictionary.ContainsKey(columnMapping.DestinationColumn.Name))
            {
                destinationColumnMappingDictionary.Add(columnMapping.DestinationColumn.Name, columnMapping.DestinationColumn);
            }
            if (!sourceColumnMappingDictionary.ContainsKey(columnMapping.DestinationColumn.Name))
            {
                sourceColumnMappingDictionary.Add(columnMapping.DestinationColumn.Name, columnMapping);
            }
        }
    }

    public void CreateTempTable(string? tempTableSchema, string tempTableName, string tempTablePrefix, List<SqlColumn> destinationColumns, ILogger logger)
    {
        SQLTable.CreateTempTable(sqlCommand, tempTableSchema, tempTableName, tempTablePrefix, destinationColumns, logger);
    }
    internal void CreateTempTables()
    {
        // refresh tables from the db
        var currentTables = ((EcomProvider)job.Destination).GetDynamicwebSourceSchema().GetTables();
        // ensure connection is open (as it is closed after GetTables() call
        var tables = job.Destination.GetSchema().GetTables();
        EnsureConnectionIsOpen();
        foreach (Table table in tables)
        {
            var currentTable = GetTable(table.Name, currentTables);
            if (DestinationColumnMappings.TryGetValue(table.Name, out Dictionary<string, Column>? columnMappingDictionary))
            {

                List<SqlColumn> destColumns = new List<SqlColumn>();
                foreach (Column column in columnMappingDictionary.Values)
                {
                    destColumns.Add((SqlColumn)column);
                }
                switch (table.Name)
                {
                    case "EcomVariantGroups":
                        EnsureDestinationColumns(currentTable, columnMappingDictionary, destColumns, ["VariantGroupID", "VariantGroupLanguageID"]);
                        break;
                    case "EcomVariantsOptions":
                        EnsureDestinationColumns(currentTable, columnMappingDictionary, destColumns, ["VariantOptionID", "VariantOptionLanguageID"]);
                        break;
                    case "EcomProducts":
                        EnsureDestinationColumns(currentTable, columnMappingDictionary, destColumns, ["ProductVariantID", "ProductID", "ProductLanguageID", "ProductDefaultShopId", "ProductVariantCounter"]);
                        break;
                    case "EcomProductCategoryFieldValue":
                        EnsureDestinationColumns(currentTable, columnMappingDictionary, destColumns, ["FieldValueFieldCategoryId", "FieldValueProductId", "FieldValueProductVariantId", "FieldValueProductLanguageId"]);
                        break;
                    case "EcomPrices":
                        EnsureDestinationColumns(currentTable, columnMappingDictionary, destColumns, ["PriceId", "PriceCurrency", "PriceShopId", "PriceUserId", "PriceUserGroupId"]);
                        break;
                    case "EcomDiscount":
                        EnsureDestinationColumns(currentTable, columnMappingDictionary, destColumns, ["DiscountAccessUserId", "DiscountAccessUserGroupId", "DiscountCurrencyCode"]);
                        break;
                    case "EcomGroups":
                        EnsureDestinationColumns(currentTable, columnMappingDictionary, destColumns, ["GroupLanguageID", "GroupID"]);
                        break;
                    case "EcomProductsRelated":
                        EnsureDestinationColumns(currentTable, columnMappingDictionary, destColumns, ["ProductRelatedProductID", "ProductRelatedProductRelID", "ProductRelatedGroupID", "ProductRelatedProductRelVariantID"]);
                        break;
                    case "EcomStockUnit":
                        EnsureDestinationColumns(currentTable, columnMappingDictionary, destColumns, ["StockUnitProductID", "StockUnitVariantID", "StockUnitID"]);
                        break;
                    case "EcomManufacturers":
                        EnsureDestinationColumns(currentTable, columnMappingDictionary, destColumns, ["ManufacturerID", "ManufacturerName"]);
                        break;
                    case "EcomDetails":
                        EnsureDestinationColumns(currentTable, columnMappingDictionary, destColumns, ["DetailID", "DetailLanguageId"]);
                        break;
                    case "EcomAssortmentPermissions":
                        EnsureDestinationColumns(currentTable, columnMappingDictionary, destColumns, ["AssortmentPermissionAccessUserID"]);
                        break;
                    case "EcomVariantOptionsProductRelation":
                        EnsureDestinationColumns(currentTable, columnMappingDictionary, destColumns, ["VariantOptionsProductRelationProductID", "VariantOptionsProductRelationVariantID"]);
                        break;
                    case "EcomAssortments":
                        EnsureDestinationColumns(currentTable, columnMappingDictionary, destColumns, ["AssortmentLanguageID"]);
                        break;
                    case "EcomAssortmentShopRelations":
                        EnsureDestinationColumns(currentTable, columnMappingDictionary, destColumns, ["AssortmentShopRelationShopID"]);
                        break;
                    case "EcomCurrencies":
                        EnsureDestinationColumns(currentTable, columnMappingDictionary, destColumns, ["CurrencyCode", "CurrencyLanguageId"]);
                        break;
                    case "EcomCountries":
                        EnsureDestinationColumns(currentTable, columnMappingDictionary, destColumns, ["CountryCode2", "CountryCultureInfo", "CountryRegionCode", "CountryVAT", "CountryCode3", "CountryCurrencyCode"]);
                        break;
                    case "EcomStockLocation":
                        EnsureDestinationColumns(currentTable, columnMappingDictionary, destColumns, ["StockLocationName", "StockLocationGroupId"]);
                        break;
                    case "EcomUnits":
                        EnsureDestinationColumns(currentTable, columnMappingDictionary, destColumns, ["UnitId", "UnitExternalId"]);
                        break;
                    case "EcomUnitTranslations":
                        EnsureDestinationColumns(currentTable, columnMappingDictionary, destColumns, ["UnitTranslationUnitId", "UnitTranslationLanguageId", "UnitTranslationName"]);
                        break;
                }
                if (Mappings.TryGetValue(table.Name, out List<Mapping>? tableMappings))
                {
                    foreach (var mapping in tableMappings)
                    {
                        CreateTempTable(table.SqlSchema, table.Name, TempTableName + mapping.GetId(), destColumns, logger);
                        AddTableToDataset(destColumns, GetTableName(table.Name, mapping));
                    }
                }
            }
            else
            {
                List<SqlColumn> destColumns = new List<SqlColumn>();
                switch (table.Name)
                {
                    //add columns to destTables for current Table, if it's needed when the table is not included in the mapping
                    case "EcomProducts":
                        break;
                    case "EcomGroups":
                        EnsureDestinationColumns(currentTable, null, destColumns, ["GroupID", "GroupLanguageID", "GroupName"]);
                        CreateTempTable(table.SqlSchema, table.Name, TempTableName, destColumns, logger);
                        AddTableToDataset(destColumns, table.Name);
                        break;
                    case "EcomVariantGroups":
                        EnsureDestinationColumns(currentTable, null, destColumns, ["VariantGroupID", "VariantGroupLanguageID", "VariantGroupName"]);
                        CreateTempTable(table.SqlSchema, table.Name, TempTableName, destColumns, logger);
                        AddTableToDataset(destColumns, table.Name);
                        break;
                    case "EcomVariantsOptions":
                        EnsureDestinationColumns(currentTable, null, destColumns, ["VariantOptionID", "VariantOptionLanguageID", "VariantOptionName"]);
                        CreateTempTable(table.SqlSchema, table.Name, TempTableName, destColumns, logger);
                        AddTableToDataset(destColumns, table.Name);
                        break;
                    case "EcomManufacturers":
                        EnsureDestinationColumns(currentTable, null, destColumns, ["ManufacturerID", "ManufacturerName"]);
                        CreateTempTable(table.SqlSchema, table.Name, TempTableName, destColumns, logger);
                        AddTableToDataset(destColumns, table.Name);
                        break;
                    case "EcomProductsRelated":
                        EnsureDestinationColumns(currentTable, null, destColumns, ["ProductRelatedProductID", "ProductRelatedProductRelID", "ProductRelatedGroupID", "ProductRelatedProductRelVariantID"]);
                        CreateTempTable(table.SqlSchema, table.Name, TempTableName, destColumns, logger);
                        AddTableToDataset(destColumns, table.Name);
                        break;
                    case "EcomLanguages":
                        EnsureDestinationColumns(currentTable, null, destColumns, ["LanguageID", "LanguageCode2", "LanguageName", "LanguageNativeName"]);
                        CreateTempTable(table.SqlSchema, table.Name, TempTableName, destColumns, logger);
                        AddTableToDataset(destColumns, table.Name);
                        break;
                    case "EcomVariantOptionsProductRelation":
                        EnsureDestinationColumns(currentTable, null, destColumns, ["VariantOptionsProductRelationProductID", "VariantOptionsProductRelationVariantID"]);
                        CreateTempTable(table.SqlSchema, table.Name, TempTableName, destColumns, logger);
                        AddTableToDataset(destColumns, table.Name);
                        break;
                    case "EcomProductCategoryFieldValue":
                        EnsureDestinationColumns(currentTable, null, destColumns, ["FieldValueFieldId", "FieldValueFieldCategoryId", "FieldValueProductId", "FieldValueProductVariantId", "FieldValueProductLanguageId", "FieldValueValue"]);
                        CreateTempTable(table.SqlSchema, table.Name, TempTableName, destColumns, logger);
                        AddTableToDataset(destColumns, table.Name);
                        break;
                }
            }
        }

        //Create product group relation temp table
        var relationTable = GetTable("EcomGroupProductRelation", currentTables);
        var groupProductRelationColumns = new List<SqlColumn>();
        EnsureDestinationColumns(relationTable, null, groupProductRelationColumns, ["GroupProductRelationGroupId", "GroupProductRelationProductId", "GroupProductRelationSorting", "GroupProductRelationIsPrimary"]);
        CreateTempTable(null, "EcomGroupProductRelation", TempTableName, groupProductRelationColumns, logger);
        AddTableToDataset(groupProductRelationColumns, "EcomGroupProductRelation");

        //create product variantgroup relation temp table
        List<SqlColumn> variantGroupProductRelation = new List<SqlColumn>();
        relationTable = GetTable("EcomVariantgroupProductRelation", currentTables);
        EnsureDestinationColumns(relationTable, null, variantGroupProductRelation, ["VariantgroupProductRelationProductID", "VariantgroupProductRelationVariantGroupID", "VariantgroupProductRelationID", "VariantGroupProductRelationSorting"]);
        CreateTempTable(null, "EcomVariantgroupProductRelation", TempTableName, variantGroupProductRelation, logger);
        AddTableToDataset(variantGroupProductRelation, "EcomVariantgroupProductRelation");

        //Create ShopGroupRelation temp table
        List<SqlColumn> shopGroupRelations = new List<SqlColumn>();
        relationTable = GetTable("EcomShopGroupRelation", currentTables);
        EnsureDestinationColumns(relationTable, null, shopGroupRelations, ["ShopGroupShopID", "ShopGroupGroupID", "ShopGroupRelationsSorting"]);
        CreateTempTable(null, "EcomShopGroupRelation", TempTableName, shopGroupRelations, logger);
        AddTableToDataset(shopGroupRelations, "EcomShopGroupRelation");

        //Create Shop relation table
        List<SqlColumn> shops = new List<SqlColumn>();
        relationTable = GetTable("EcomShops", currentTables);
        EnsureDestinationColumns(relationTable, null, shops, ["ShopID", "ShopName"]);
        CreateTempTable(null, "EcomShops", TempTableName, shops, logger);
        AddTableToDataset(shops, "EcomShops");

        //Create Product-relatedGroup temp table
        List<SqlColumn> productsRelatedGroups = new List<SqlColumn>();
        relationTable = GetTable("EcomProductsRelatedGroups", currentTables);
        EnsureDestinationColumns(relationTable, null, productsRelatedGroups, ["RelatedGroupID", "RelatedGroupName", "RelatedGroupLanguageID"]);
        CreateTempTable(null, "EcomProductsRelatedGroups", TempTableName, productsRelatedGroups, logger);
        AddTableToDataset(productsRelatedGroups, "EcomProductsRelatedGroups");

        //Create EcomGroupRelations temp table
        List<SqlColumn> groupRelations = new List<SqlColumn>();
        relationTable = GetTable("EcomGroupRelations", currentTables);
        EnsureDestinationColumns(relationTable, null, groupRelations, ["GroupRelationsGroupID", "GroupRelationsParentID", "GroupRelationsSorting"]);
        CreateTempTable(null, "EcomGroupRelations", TempTableName, groupRelations, logger);
        AddTableToDataset(groupRelations, "EcomGroupRelations");
    }

    private void EnsureConnectionIsOpen()
    {
        if (connection.State != ConnectionState.Open)
        {
            connection.Open();
        }
    }

    private Table GetTable(string tableName, TableCollection tables)
    {
        var currentTable = tables.FirstOrDefault(t => string.Equals(t.Name, tableName, StringComparison.OrdinalIgnoreCase));
        Ensure.NotNull(currentTable, $"Can not find the table: {tableName} in the database schema");
        return currentTable;
    }

    private static void EnsureDestinationColumns(Table table, Dictionary<string, Column>? columnMappingDictionary, List<SqlColumn> destColumns, string[] columnsToAdd)
    {
        var columns = columnMappingDictionary != null ? columnsToAdd.Where(c => !columnMappingDictionary.ContainsKey(c)) : columnsToAdd;
        foreach (var columnName in columns)
        {
            var column = table.Columns.FirstOrDefault(c => string.Equals(c.Name, columnName, StringComparison.OrdinalIgnoreCase));
            if (column is not null && column is SqlColumn sqlColumn)
            {
                destColumns.Add(sqlColumn);
            }
        }
    }

    private string GetTableName(string name, Mapping mapping)
    {
        return $"{name}${mapping.GetId()}";
    }

    private string GetTableNameWithoutPrefix(string name)
    {
        if (name.Contains("$"))
        {
            return name.Split(new char[] { '$' })[0];
        }
        else
        {
            return name;
        }
    }

    private string GetPrefixFromTableName(string name)
    {
        if (name.Contains("$"))
        {
            return name.Split(new char[] { '$' })[1];
        }
        else
        {
            return string.Empty;
        }
    }

    private void AddTableToDataset(IEnumerable<SqlColumn> groupProductRelationColumns, string tableName)
    {
        var newTable = DataToWrite.Tables.Add(tableName);
        foreach (SqlColumn destColumn in groupProductRelationColumns)
        {
            newTable.Columns.Add(destColumn.Name, destColumn.Type);
        }
        DataRowsToWrite.Add(tableName, new Dictionary<string, List<DataRow>>());
    }

    private readonly string _defaultLanguageId;
    private int _lastVariantGroupProductRelationID = -1;
    private int LastVariantGroupProductRelationID
    {
        get
        {
            if (_lastVariantGroupProductRelationID == -1)
            {
                _lastVariantGroupProductRelationID = GetLastId(CommandBuilder.Create("(select convert(nvarchar,(MAX(CAST(SUBSTRING(VariantgroupProductRelationID,22,LEN(VariantgroupProductRelationID)-21) as int)+1))) as lastID from EcomVariantGroupProductRelation where VariantgroupProductRelationID like 'ImportedVARGRPPRODREL%')"));
            }
            return _lastVariantGroupProductRelationID;
        }
        set { _lastVariantGroupProductRelationID = value; }
    }

    private int _variantGroupProductRelationSortingCounter = 0;

    private int _lastRelatedGroupID = -1;
    protected int LastRelatedGroupID
    {
        get
        {
            if (_lastRelatedGroupID == -1)
            {
                _lastRelatedGroupID = GetLastId(CommandBuilder.Create("(select convert(nvarchar,MAX(CAST(SUBSTRING(RelatedGroupID,15,LEN(RelatedGroupID)-14) as int))) as lastID from EcomProductsRelatedGroups where RelatedGroupID like 'ImportedRELGRP%')"));
            }
            return _lastRelatedGroupID;
        }
        set { _lastRelatedGroupID = value; }
    }

    private int _lastShopId = -1;
    private int LastShopId
    {
        get
        {
            if (_lastShopId == -1)
            {
                _lastShopId = GetLastId(CommandBuilder.Create("(select convert(nvarchar,(MAX(CAST(SUBSTRING( ShopID ,13, LEN(ShopID)-12) as int)+1))) as lastID from EcomShops where ShopID like 'ImportedSHOP%')"));
            }
            return _lastShopId;
        }
        set { _lastShopId = value; }
    }

    private int _lastProductId = -1;
    private int LastProductId
    {
        get
        {
            if (_lastProductId == -1)
            {
                _lastProductId = GetLastId(CommandBuilder.Create("(select convert(nvarchar,MAX(CAST(SUBSTRING(ProductID,13,LEN(ProductID)-12) as int))) as lastID from EcomProducts where ProductID like 'ImportedPROD%')"));
            }
            return _lastProductId;
        }
        set { _lastProductId = value; }
    }

    private int _lastGroupId = -1;
    protected int LastGroupId
    {
        get
        {
            if (_lastGroupId == -1)
            {
                _lastGroupId = GetLastId(CommandBuilder.Create("(select convert(nvarchar,(MAX(CAST(SUBSTRING( GroupID ,14,LEN(GroupID)-13) as int)+1))) as lastID from EcomGroups where GroupID like 'ImportedGROUP%')"));
            }
            return _lastGroupId;

        }
        set { _lastGroupId = value; }
    }

    private int _lastManufacturerId = -1;
    protected int LastManufacturerId
    {
        get
        {
            if (_lastManufacturerId == -1)
            {
                _lastManufacturerId = GetLastId(CommandBuilder.Create("(select convert(nvarchar,(MAX(CAST(SUBSTRING( ManufacturerID ,13,LEN(ManufacturerID)-12) as int)))) as lastID from EcomManufacturers where ManufacturerID like 'ImportedMANU%')"));
            }
            return _lastManufacturerId;

        }
        set { _lastManufacturerId = value; }
    }

    private int _lastVariantGroupId = -1;
    private int LastVariantGroupId
    {
        get
        {
            if (_lastVariantGroupId == -1)
            {
                _lastVariantGroupId = GetLastId(CommandBuilder.Create("(select convert(nvarchar,(MAX(CAST(SUBSTRING( variantGroupId ,15,LEN(variantGroupId)-14) as int)+1))) as lastID from EcomVariantGroups where VariantGroupID like 'ImportedVARGRP%')"));
            }
            return _lastVariantGroupId;
        }
        set { _lastVariantGroupId = value; }
    }

    private int _lastVariantOptionId = -1;
    private int LastVariantOptionId
    {
        get
        {
            if (_lastVariantOptionId == -1)
            {
                _lastVariantOptionId = GetLastId(CommandBuilder.Create("(select convert(nvarchar,(MAX(CAST(SUBSTRING( variantOptionId ,11,LEN(variantOptionId)-10) as int)+1))) as lastID from EcomVariantsOptions where VariantOptionID like 'ImportedVO%')"));
            }
            return _lastVariantOptionId;
        }
        set { _lastVariantOptionId = value; }
    }

    private int _lastLanguageId = -1;
    protected int LastLanguageId
    {
        get
        {
            if (_lastLanguageId == -1)
            {
                _lastLanguageId = GetLastId(CommandBuilder.Create("(select convert(nvarchar,(MAX(CAST(SUBSTRING( LanguageID ,13,LEN(LanguageID)-12) as int)+1))) as lastID from EcomLanguages where LanguageID like 'ImportedLANG%')"));
            }
            return _lastLanguageId;

        }
        set { _lastLanguageId = value; }
    }

    private int _lastPriceId = -1;
    private int LastPriceId
    {
        get
        {
            if (_lastPriceId == -1)
            {
                _lastPriceId = GetLastId(CommandBuilder.Create("(select convert(nvarchar,MAX(CAST(SUBSTRING(PriceID,14,LEN(PriceID)-13) as int))) as lastID from EcomPrices where PriceID like 'ImportedPRICE%')"));
            }
            return _lastPriceId;
        }
        set { _lastPriceId = value; }
    }

    private int _lastDetailId = -1;
    private int LastDetailId
    {
        get
        {
            if (_lastDetailId == -1)
            {
                _lastDetailId = GetLastId(CommandBuilder.Create("(select convert(nvarchar,MAX(CAST(SUBSTRING(DetailID,15,LEN(DetailID)-14) as int))) as lastID from EcomDetails where DetailID like 'ImportedDETAIL%')"));
            }
            return _lastDetailId;
        }
        set { _lastDetailId = value; }
    }

    private int _lastStockLocationGroupId = -1;
    private int GetLastStockLocationGroupId()
    {
        if (_lastStockLocationGroupId == -1)
        {
            _lastStockLocationGroupId = GetLastId(CommandBuilder.Create("(select convert(nvarchar,MAX(CAST(StockLocationGroupId as int))) as lastID from EcomStockLocation)"));
        }
        _lastStockLocationGroupId++;
        return _lastStockLocationGroupId;
    }

    private int GetLastId(CommandBuilder commandBuilder)
    {
        using (var reader = Database.CreateDataReader(commandBuilder, sqlCommand.Connection))
        {
            if (reader.Read())
            {
                return reader["lastID"] != DBNull.Value ? Converter.ToInt32(reader["lastID"]) : 0;
            }
            return 0;
        }
    }

    internal Dictionary<string, DataRow>? _variantGroups = null;
    private Dictionary<string, DataRow> VariantGroups
    {
        get
        {
            if (_variantGroups == null)
            {
                _variantGroups = new Dictionary<string, DataRow>(StringComparer.OrdinalIgnoreCase);
                DataSet dataSet = Database.CreateDataSet(CommandBuilder.Create("select variantGroupID, variantgroupName, VariantGroupLanguageID from EcomVariantGroups"), sqlCommand.Connection);
                foreach (DataRow row in dataSet.Tables[0].Rows)
                {
                    var rowId = Converter.ToString(row["variantGroupID"]);
                    if (!_variantGroups.ContainsKey(rowId))
                    {
                        _variantGroups.Add(rowId, row);
                    }
                }
            }
            return _variantGroups;
        }
    }

    internal HashSet<string?>? _existingVariantOptionsList;
    private HashSet<string?> GetVariantOptionList()
    {
        if (_existingVariantOptionsList == null)
        {
            _existingVariantOptionsList = new HashSet<string?>();
            using (var reader = Database.CreateDataReader(CommandBuilder.Create("select VariantOptionID from EcomVariantsOptions"), sqlCommand.Connection))
            {
                while (reader.Read())
                {
                    _existingVariantOptionsList.Add(reader["VariantOptionID"].ToString());
                }
            }
        }
        return _existingVariantOptionsList;
    }

    internal Dictionary<string, DataRow>? _ecomLanguages;
    private Dictionary<string, DataRow> EcomLanguages
    {
        get
        {
            if (_ecomLanguages == null)
            {
                _ecomLanguages = new Dictionary<string, DataRow>(StringComparer.OrdinalIgnoreCase);
                DataSet dataSet = Database.CreateDataSet(CommandBuilder.Create("select LanguageID, LanguageCode2, LanguageName from EcomLanguages"), sqlCommand.Connection);
                foreach (DataRow row in dataSet.Tables[0].Rows)
                {
                    var languageId = Converter.ToString(row["LanguageID"]);
                    _ecomLanguages.Add(languageId, row);
                }
            }
            return _ecomLanguages;
        }
    }

    internal Dictionary<string, List<DataRow>>? _productsRelatedGroups;
    protected Dictionary<string, List<DataRow>> ProductsRelatedGroups
    {
        get
        {
            if (_productsRelatedGroups == null)
            {
                _productsRelatedGroups = new Dictionary<string, List<DataRow>>(StringComparer.OrdinalIgnoreCase);
                DataSet dataSet = Database.CreateDataSet(CommandBuilder.Create("select RelatedGroupID, RelatedGroupName, RelatedGroupLanguageID from EcomProductsRelatedGroups"), sqlCommand.Connection);
                foreach (DataRow row in dataSet.Tables[0].Rows)
                {
                    var rowId = Converter.ToString(row["RelatedGroupID"]);
                    if (!_productsRelatedGroups.TryGetValue(rowId, out List<DataRow>? rows))
                    {
                        rows = new List<DataRow>();
                        _productsRelatedGroups.Add(rowId, rows);
                    }
                    rows.Add(row);
                }
            }
            return _productsRelatedGroups;

        }
    }

    internal Dictionary<string, DataRow>? _productManufacturers;
    protected Dictionary<string, DataRow> ProductManufacturers
    {
        get
        {
            if (_productManufacturers == null)
            {
                _productManufacturers = new Dictionary<string, DataRow>(StringComparer.OrdinalIgnoreCase);
                DataSet dataSet = Database.CreateDataSet(CommandBuilder.Create("select ManufacturerID, ManufacturerName from EcomManufacturers"), sqlCommand.Connection);
                foreach (DataRow row in dataSet.Tables[0].Rows)
                {
                    var rowId = Converter.ToString(row["ManufacturerID"]);
                    if (!_productManufacturers.ContainsKey(rowId))
                    {
                        _productManufacturers.Add(rowId, row);
                    }
                }
            }
            return _productManufacturers;

        }
    }

    internal Dictionary<string, DataRow>? _ecomShops;
    private Dictionary<string, DataRow> EcomShops
    {
        get
        {
            if (_ecomShops == null)
            {
                _ecomShops = new Dictionary<string, DataRow>(StringComparer.OrdinalIgnoreCase);
                DataSet dataSet = Database.CreateDataSet(CommandBuilder.Create("select ShopID, ShopName from EcomShops"), sqlCommand.Connection);
                foreach (DataRow row in dataSet.Tables[0].Rows)
                {
                    var shopId = Converter.ToString(row["ShopID"]);
                    _ecomShops.Add(shopId, row);
                }
            }
            return _ecomShops;
        }
    }

    internal Dictionary<string, List<DataRow>>? _productGroups;
    protected Dictionary<string, List<DataRow>> ProductGroups
    {
        get
        {
            if (_productGroups == null)
            {
                _productGroups = new Dictionary<string, List<DataRow>>(StringComparer.OrdinalIgnoreCase);
                DataSet dataSet = Database.CreateDataSet(CommandBuilder.Create("select GroupID, GroupName, GroupNumber from EcomGroups"), sqlCommand.Connection);
                foreach (DataRow row in dataSet.Tables[0].Rows)
                {
                    var rowId = Converter.ToString(row["GroupID"]);
                    if (!_productGroups.TryGetValue(rowId, out List<DataRow>? rows))
                    {
                        rows = new List<DataRow>();
                        _productGroups.Add(rowId, rows);
                    }
                    rows.Add(row);
                }
            }
            return _productGroups;

        }
    }

    internal Dictionary<string, DataRow>? _productCategoryFields;
    protected Dictionary<string, DataRow> ProductCategoryFields
    {
        get
        {
            if (_productCategoryFields == null)
            {
                _productCategoryFields = new Dictionary<string, DataRow>(StringComparer.OrdinalIgnoreCase);
                DataSet dataSet = Database.CreateDataSet(CommandBuilder.Create("SELECT FieldId, FieldCategoryId FROM EcomProductCategoryField"), sqlCommand.Connection);
                foreach (DataRow row in dataSet.Tables[0].Rows)
                {
                    var fieldId = Converter.ToString(row["FieldId"]);
                    var categoryId = Converter.ToString(row["FieldCategoryId"]);

                    var rowId = $"{categoryId}|{fieldId}";
                    if (!_productCategoryFields.ContainsKey(rowId))
                    {
                        _productCategoryFields.Add(rowId, row);
                    }
                }
            }
            return _productCategoryFields;
        }
    }

    internal readonly string _sortingKeySeparator = ";";
    internal Dictionary<string, int>? _shopGroupRelationSorting = null;
    protected Dictionary<string, int> ShopGroupRelationSorting
    {
        get
        {
            if (_shopGroupRelationSorting == null)
            {
                _shopGroupRelationSorting = new Dictionary<string, int>(StringComparer.OrdinalIgnoreCase);
                DataSet dataSet = Database.CreateDataSet(CommandBuilder.Create("select ShopGroupShopID, ShopGroupGroupID, ShopGroupRelationsSorting from EcomShopGroupRelation"), sqlCommand.Connection);
                foreach (DataRow row in dataSet.Tables[0].Rows)
                {
                    var shopId = Converter.ToString(row["ShopGroupShopID"]);
                    var groupId = Converter.ToString(row["ShopGroupGroupID"]);
                    var sort = Converter.ToInt32(row["ShopGroupRelationsSorting"]);
                    _shopGroupRelationSorting.Add($"{shopId}{_sortingKeySeparator}{groupId}", sort);
                }
            }
            return _shopGroupRelationSorting;
        }
    }

    internal Dictionary<string, int>? _groupRelationSorting = null;
    protected Dictionary<string, int> GroupRelationSorting
    {
        get
        {
            if (_groupRelationSorting == null)
            {
                _groupRelationSorting = new Dictionary<string, int>(StringComparer.OrdinalIgnoreCase);
                DataSet dataSet = Database.CreateDataSet(CommandBuilder.Create("select GroupRelationsGroupId, GroupRelationsParentId, GroupRelationsSorting from EcomGroupRelations where GroupRelationsSorting > 0"), sqlCommand.Connection);
                foreach (DataRow row in dataSet.Tables[0].Rows)
                {
                    var groupId = Converter.ToString(row["GroupRelationsGroupId"]);
                    var parentId = Converter.ToString(row["GroupRelationsParentId"]);
                    var sort = Converter.ToInt32(row["GroupRelationsSorting"]);
                    _groupRelationSorting.Add($"{groupId}{_sortingKeySeparator}{parentId}", sort);
                }
            }
            return _groupRelationSorting;
        }
    }

    internal DataTable? _existingProducts;
    protected DataTable ExistingProducts
    {
        get
        {
            if (_existingProducts == null)
            {
                List<string> columnsToSelect = new List<string>() {
                    "ProductID", "ProductLanguageID", "ProductVariantID","ProductNumber", "ProductName", "ProductVariantCounter"
                };
                IEnumerable<string> ecomProductsPKColumns = MappingIdEcomProductsPKColumns.Values.SelectMany(i => i);
                if (ecomProductsPKColumns != null)
                {
                    IEnumerable<string> columnsToAdd = ecomProductsPKColumns.Where(c =>
                        !columnsToSelect.Any(cs => string.Equals(c, cs, StringComparison.OrdinalIgnoreCase)));
                    columnsToSelect.AddRange(columnsToAdd);
                }
                string sql = "select " + string.Join(",", columnsToSelect) + " from EcomProducts";
                DataSet dataSet = Database.CreateDataSet(CommandBuilder.Create(sql), sqlCommand.Connection);
                _existingProducts = dataSet.Tables[0];
            }
            return _existingProducts;
        }
    }

    private Dictionary<string, (string ProductId, string ProductVariantId)>? _productNumberVariantIds;
    protected Dictionary<string, (string ProductId, string ProductVariantId)> ProductNumberVariantIds
    {
        get
        {
            if (_productNumberVariantIds == null)
            {
                _productNumberVariantIds = new Dictionary<string, (string ProductId, string ProductVariantId)>();

                if (_useProductIdFoundByNumber)
                {
                    foreach (var row in ExistingProducts.Select("ProductNumber <> '' and ProductVariantID <> '' and ProductLanguageID = '" + _defaultLanguageId + "'"))
                    {
                        string number = row["ProductNumber"].ToString() ?? "";
                        if (!_productNumberVariantIds.ContainsKey(number))
                        {
                            _productNumberVariantIds.Add(number, (row["ProductID"].ToString() ?? "", row["ProductVariantID"].ToString() ?? ""));
                        }
                    }
                }
            }
            return _productNumberVariantIds;
        }
    }

    internal Dictionary<string, Tuple<bool, int>>? _existingGroupProductRelations = null;
    protected Dictionary<string, Tuple<bool, int>> ExistingGroupProductRelations
    {
        get
        {
            if (_existingGroupProductRelations == null)
            {
                _existingGroupProductRelations = new Dictionary<string, Tuple<bool, int>>(StringComparer.OrdinalIgnoreCase);
                DataSet dataSet = Database.CreateDataSet(CommandBuilder.Create("select GroupProductRelationGroupID, GroupProductRelationProductID, GroupProductRelationIsPrimary, GroupProductRelationSorting from EcomGroupProductRelation where GroupProductRelationSorting <> 0"), sqlCommand.Connection);
                foreach (DataRow row in dataSet.Tables[0].Rows)
                {
                    string productId = Converter.ToString(row["GroupProductRelationProductId"]);
                    string groupId = Converter.ToString(row["GroupProductRelationGroupId"]);
                    bool isPrimary = Converter.ToBoolean(row["GroupProductRelationIsPrimary"]);
                    int sort = Converter.ToInt32(row["GroupProductRelationSorting"]);
                    string key = $"{groupId}{_sortingKeySeparator}{productId}";
                    _existingGroupProductRelations.Add(key, new Tuple<bool, int>(isPrimary, sort));
                }
            }
            return _existingGroupProductRelations;
        }
    }

    internal Dictionary<string, DataRow>? _primaryGroupProductRelations = null;
    protected Dictionary<string, DataRow> PrimaryGroupProductRelations
    {
        get
        {
            if (_primaryGroupProductRelations == null)
            {
                _primaryGroupProductRelations = new Dictionary<string, DataRow>(StringComparer.OrdinalIgnoreCase);
                DataSet dataSet = Database.CreateDataSet(CommandBuilder.Create("select GroupProductRelationGroupID, GroupProductRelationProductID, GroupProductRelationSorting from EcomGroupProductRelation where GroupProductRelationIsPrimary = 1"), sqlCommand.Connection);
                foreach (DataRow row in dataSet.Tables[0].Rows)
                {
                    var rowId = Converter.ToString(row["GroupProductRelationProductID"]);
                    if (!_primaryGroupProductRelations.ContainsKey(rowId))
                    {
                        _primaryGroupProductRelations.Add(rowId, row);
                    }
                }
            }
            return _primaryGroupProductRelations;

        }
    }

    private DataTable? _existingUserGroups;
    private DataTable ExistingUserGroups
    {
        get
        {
            if (_existingUserGroups == null)
            {
                DataSet dataSet = Database.CreateDataSet(CommandBuilder.Create("select * from AccessUser where AccessUserType IN (2,10,11)"), sqlCommand.Connection);
                _existingUserGroups = dataSet.Tables[0];
            }
            return _existingUserGroups;
        }
    }

    private DataTable? _existingUsers;
    private DataTable ExistingUsers
    {
        get
        {
            if (_existingUsers == null)
            {
                DataSet dataSet = Database.CreateDataSet(CommandBuilder.Create("select AccessUserID, AccessUserUserName, AccessUserCustomerNumber, AccessUserExternalID from AccessUser"), sqlCommand.Connection);
                _existingUsers = dataSet.Tables[0];
            }
            return _existingUsers;
        }
    }

    private DataTable? _existingUnits;
    private DataTable ExistingUnits
    {
        get
        {
            if (_existingUnits == null)
            {
                DataSet dataSet = Database.CreateDataSet(CommandBuilder.Create("SELECT UnitId, UnitExternalId FROM EcomUnits"), sqlCommand.Connection);
                _existingUnits = dataSet.Tables[0];
            }
            return _existingUnits;
        }
    }

    private DataTable? _existingUnitTranslations;
    private DataTable ExistingUnitTranslations
    {
        get
        {
            if (_existingUnitTranslations == null)
            {
                DataSet dataSet = Database.CreateDataSet(CommandBuilder.Create("SELECT UnitTranslationUnitId, UnitTranslationLanguageId, UnitTranslationName FROM EcomUnitTranslations"), sqlCommand.Connection);
                _existingUnitTranslations = dataSet.Tables[0];
            }
            return _existingUnitTranslations;
        }
    }

    private int _currentlyWritingMappingId = 0;
    private long _writtenRowsCount = 0;

    public void Write(Dictionary<string, object> row, Mapping mapping, bool discardDuplicates)
    {
        DataRow? dataRow = DataToWrite.Tables[GetTableName(mapping.DestinationTable.Name, mapping)]?.NewRow();
        if (dataRow is null)
            return;

        var mappingColumns = ColumnMappingsByMappingId[mapping.GetId()];
        foreach (ColumnMapping columnMapping in mappingColumns)
        {
            if ((columnMapping.DestinationColumn is not null && columnMapping.SourceColumn is not null && row.ContainsKey(columnMapping.SourceColumn.Name)) || columnMapping.HasScriptWithValue)
            {
                if (mapping.DestinationTable.Name.Equals("EcomStockUnit", StringComparison.OrdinalIgnoreCase) && columnMapping.DestinationColumn is not null && columnMapping.DestinationColumn.Name.Equals("StockUnitStockLocationId", StringComparison.OrdinalIgnoreCase))
                {
                    var stockLocationID = GetStockLocationIdByName(row, columnMapping);
                    dataRow[columnMapping.DestinationColumn.Name] = stockLocationID;
                    if (columnMapping.SourceColumn is not null)
                        row[columnMapping.SourceColumn.Name] = stockLocationID;
                }

                if (columnMapping.DestinationColumn is not null && columnMapping.SourceColumn is not null && mappingColumns.Any(obj => obj.DestinationColumn.Name == columnMapping.DestinationColumn.Name && obj.GetId() < columnMapping.GetId()))
                {
                    dataRow[columnMapping.DestinationColumn.Name] += columnMapping.ConvertInputValueToOutputValue(row[columnMapping.SourceColumn.Name]) + "";
                }
                else if (columnMapping.HasScriptWithValue && columnMapping.DestinationColumn is not null)
                {
                    dataRow[columnMapping.DestinationColumn.Name] = columnMapping.GetScriptValue();
                }
                else
                {
                    var rowValue = columnMapping.SourceColumn is not null ? row[columnMapping.SourceColumn.Name] : null;
                    if (IsColumnNullableAndValueNull(columnMapping, rowValue))
                        continue;
                    if (columnMapping.DestinationColumn is not null && columnMapping.SourceColumn is not null)
                        dataRow[columnMapping.DestinationColumn.Name] = columnMapping.ConvertInputValueToOutputValue(row[columnMapping.SourceColumn.Name]);
                }
            }
            else
            {
                if (columnMapping.Active)
                {
                    if (columnMapping.DestinationColumn == null)
                    {
                        logger.Error($"The DestinationColumn is null for the table mapping {mapping.SourceTable?.Name} to {mapping.DestinationTable?.Name} on SourceColumn {columnMapping.SourceColumn?.Name}");
                    }
                    if (columnMapping.SourceColumn == null && !columnMapping.HasScriptWithValue)
                    {
                        logger.Error($"The SourceColumn is null for the table mapping {mapping.SourceTable?.Name} to {mapping.DestinationTable?.Name} on DestinationColumn {columnMapping.DestinationColumn?.Name}");
                    }
                    throw new Exception(GetRowValueNotFoundMessage(row, columnMapping.SourceColumn?.Table?.Name ?? columnMapping.DestinationColumn?.Table?.Name,
                        columnMapping.SourceColumn?.Name ?? columnMapping.DestinationColumn?.Name));
                }
            }
        }

        Dictionary<string, ColumnMapping> columnMappings = SourceColumnMappings[mapping.GetId()];
        switch (mapping.DestinationTable.Name)
        {
            case "EcomProductCategoryFieldValue":
                WriteCategoyFieldValues(row, columnMappings, dataRow);
                break;
            case "EcomVariantGroups":
                WriteVariantGroups(row, columnMappings, dataRow);
                duplicateRowsHandler ??= new DuplicateRowsHandler(logger, job.Mappings);
                break;
            case "EcomVariantsOptions":
                WriteVariantOptions(row, columnMappings, dataRow);
                break;
            case "EcomManufacturers":
                WriteManufacturers(row, columnMappings, dataRow);
                break;
            case "EcomGroups":
                WriteGroups(row, columnMappings, dataRow);
                break;
            case "EcomProducts":
                if (!WriteProducts(row, mapping, columnMappings, dataRow))
                {
                    return;
                }
                break;
            case "EcomProductsRelated":
                WriteRelatedProducts(row, columnMappings, dataRow);
                break;
            case "EcomPrices":
                WritePrices(row, columnMappings, dataRow);
                break;
            case "EcomDetails":
                WriteDetails(row, columnMappings, dataRow);
                break;
            case "EcomAssortmentPermissions":
                if (!WriteAssortments(row, columnMappings, dataRow, mappingColumns, mapping))
                {
                    return;
                }
                break;
            case "EcomStockUnit":
                WriteStockUnits(row, columnMappings, dataRow, mapping);
                break;
            case "EcomCountries":
                WriteCountries(row, columnMappings, dataRow, mapping);
                break;
            case "EcomStockLocation":
                WriteStockLocations(row, columnMappings, dataRow);
                break;
            case "EcomDiscount":
                WriteDiscounts(row, columnMappings, dataRow);
                break;
            case "EcomUnits":
                WriteUnits(row, columnMappings, dataRow);
                break;
            case "EcomUnitTranslations":
                WriteUnitTranslations(row, columnMappings, dataRow);
                break;
        }

        foreach (ColumnMapping columnMapping in mappingColumns)
        {
            object? rowValue = null;
            if (columnMapping.HasScriptWithValue || row.TryGetValue(columnMapping.SourceColumn?.Name ?? "", out rowValue))
            {
                if (IsColumnNullableAndValueNull(columnMapping, rowValue))
                    continue;

                object dataToRow = columnMapping.ConvertInputValueToOutputValue(rowValue);

                if (mappingColumns.Any(obj => obj.DestinationColumn.Name == columnMapping.DestinationColumn.Name && obj.GetId() < columnMapping.GetId()))
                {
                    dataRow[columnMapping.DestinationColumn.Name] += dataToRow.ToString();
                }
                else
                {
                    dataRow[columnMapping.DestinationColumn.Name] = dataToRow;
                }
            }
        }
        if (!discardDuplicates || (duplicateRowsHandler is not null && !duplicateRowsHandler.IsRowDuplicate(mappingColumns.Where(cm => cm.Active), mapping, dataRow, row)))
        {
            var tableName = GetTableName(mapping.DestinationTable.Name, mapping);
            var tableKey = GetTableKey(mapping.DestinationTable.Name);
            if (!string.IsNullOrWhiteSpace(tableKey))
            {
                var rowId = Converter.ToString(dataRow[tableKey]);
                if (!DataRowsToWrite[tableName].TryGetValue(rowId, out List<DataRow>? rows))
                {
                    rows = new List<DataRow>();

                    DataRowsToWrite[tableName].Add(rowId, rows);
                }
                rows.Add(dataRow);
            }
            else
            {
                DataRowsToWrite[tableName].Add(RowAutoId++.ToString(), new List<DataRow>() { dataRow });
            }

            if (_currentlyWritingMappingId != mapping.GetId())
            {
                _currentlyWritingMappingId = mapping.GetId();
                _writtenRowsCount = 0;
            }
            if (++_writtenRowsCount % 10000 == 0)
            {
                logger.Log("Added " + _writtenRowsCount + " rows to temporary table for " + mapping.DestinationTable.Name + ".");
            }

            assortmentHandler?.ProcessAssortments(dataRow, mapping);
        }
    }

    private static bool IsColumnNullableAndValueNull(ColumnMapping columnMapping, object? rowValue) => Nullable.GetUnderlyingType(columnMapping.DestinationColumn.Type) == null
                            && (rowValue is null || string.IsNullOrEmpty(rowValue.ToString()))
                            && columnMapping.DestinationColumn.Type != typeof(string);

    private string GetTableKey(string name)
    {
        switch (name)
        {
            case "EcomVariantGroups":
                return "VariantGroupID";
            case "EcomVariantsOptions":
                return "VariantOptionID";
            case "EcomManufacturers":
                return "ManufacturerID";
            case "EcomGroups":
                return "GroupID";

            case "EcomProductsRelatedGroups":
                return "RelatedGroupID";
            case "EcomLanguages":
                return "LanguageID";

            default:
                return string.Empty;
        }
    }

    private bool WriteProducts(Dictionary<string, object> row, Mapping mapping, Dictionary<string, ColumnMapping> columnMappings, DataRow dataRow)
    {
        //Create ID if missing  
        DataRow? existingProductRow = null;
        string productLanguageID = HandleProductLanguageId(row, columnMappings, dataRow);
        string productID = HandleProductId(row, mapping, columnMappings, dataRow, ref existingProductRow, productLanguageID);
        string productVariantID = HandleVariantId(row, columnMappings, dataRow, existingProductRow, ref productID);

        if (existingProductRow != null)
        {
            dataRow["ProductVariantCounter"] = existingProductRow["ProductVariantCounter"];
        }

        //Find groups, create if missing, add relations   
        HandleProductGroups(row, columnMappings, productID, productLanguageID, dataRow.Table);

        HandleProductCategoryFields(row, columnMappings, productID, productVariantID, productLanguageID);

        //Find Manufacturer, create if missing, add Manufacturer Reference
        HandleProductManufacturers(row, columnMappings, dataRow);

        //Find VariantRelations, create if missing, Add Variant Relations
        string? variantGroupsString = HandleProductVariantGroups(row, columnMappings, productID, dataRow.Table);

        //Find VariantRelations, create if missing, Add Variant Relations
        string? variantOptionsString = HandleProductVariantOptions(row, columnMappings, productID, dataRow.Table);

        if (string.IsNullOrEmpty(variantGroupsString) && string.IsNullOrEmpty(variantOptionsString) && !string.IsNullOrEmpty(productVariantID))
        {
            CountProductVariantGroups(productID, productVariantID);
        }

        string processedProductKey = string.Format("{0}.{1}.{2}", productID, productVariantID, productLanguageID);
        if (_processedProductsKeys.ContainsKey(processedProductKey))
        {
            //_logger.Log(string.Format("Skipped product row with [ProductID.VariantID.LanguageID] combination = '{0}' as it is already exists. Source row data: '{1}'.", processedProductKey, BaseProvider.GetFailedSourceRowMessage(row)));
            return false;
        }
        else
        {
            _processedProductsKeys.Add(processedProductKey, string.Empty);
        }
        string productKey = string.Format("{0}.{1}", productID, productLanguageID);
        int productVariantsCount = 0;
        if (ProductVariantsCountDictionary.TryGetValue(productKey, out productVariantsCount))
        {
            ProductVariantsCountDictionary[productKey] = productVariantsCount + 1;
        }
        else
        {
            ProductVariantsCountDictionary[productKey] = 1;
        }
        return true;
    }

    private void HandleProductIdFoundByNumber(Dictionary<string, object> row, Dictionary<string, ColumnMapping> columnMappings, DataRow dataRow, ref string productId, ref string productVariantId)
    {
        if (_useProductIdFoundByNumber)
        {
            columnMappings.TryGetValue("ProductNumber", out ColumnMapping? column);
            string? productNumber = GetValue(column, row);
            if (!string.IsNullOrEmpty(productNumber) &&
                ProductNumberVariantIds.TryGetValue(productNumber, out (string ProductId, string VariantId) id))
            {
                productId = id.ProductId;
                productVariantId = id.VariantId;
                dataRow["ProductID"] = productId;
                dataRow["ProductVariantID"] = productVariantId;
            }
        }
    }

    private string? HandleProductVariantOptions(Dictionary<string, object> row, Dictionary<string, ColumnMapping> columnMappings, string productID, DataTable dataTable)
    {
        ColumnMapping? column = null;
        columnMappings.TryGetValue("VariantOptions", out column);
        string? variantOptionsString = GetMergedValue(column, row);
        if (!string.IsNullOrEmpty(variantOptionsString))
        {
            var variantOptionIds = SplitOnComma(variantOptionsString);
            for (int i = 0; i < variantOptionIds.Length; i++)
            {
                string variantOption = variantOptionIds[i];

                string key = string.Format("{0}.{1}", productID.ToLower(), variantOption.ToLower());
                if (!ecomVariantOptionsProductRelationKeys.ContainsKey(key))
                {
                    foreach (string option in variantOption.Split('.'))
                    {
                        if (!GetVariantOptionList().Contains(option))
                        {
                            var id = option.Replace("'", "''");
                            if (FindRow("EcomVariantsOptions", id, dataTable.Columns.Contains("VariantOptionName") ? new Func<DataRow, bool>(r => r["VariantOptionName"].ToString() == id) : null) == null)
                            {
                                throw new Exception("Relation betweeen product \"" + productID + "\" and VariantOption \"" + variantOption + "\" can not be created. The VariantOption does not exist.");
                            }
                        }
                    }
                    WriteVariantOptionRelation(productID, variantOption);
                    ecomVariantOptionsProductRelationKeys.Add(key, null);
                    HandleVariantsCount(productID, variantOption);
                }
            }
        }

        return variantOptionsString;
    }

    private string? HandleProductVariantGroups(Dictionary<string, object> row, Dictionary<string, ColumnMapping> columnMappings, string productID, DataTable dataTable)
    {
        ColumnMapping? column = null;
        columnMappings.TryGetValue("VariantGroups", out column);
        string? variantGroupsString = GetValue(column, row);
        if (!string.IsNullOrEmpty(variantGroupsString))
        {
            var variantGroupId = SplitOnComma(variantGroupsString);
            for (int i = 0; i < variantGroupId.Length; i++)
            {
                string variantGroup = variantGroupId[i];
                AddVariantGroupReferenceToProductByVariantGroupName(productID, variantGroup, dataTable);
            }
        }

        return variantGroupsString;
    }

    private void HandleProductManufacturers(Dictionary<string, object> row, Dictionary<string, ColumnMapping> columnMappings, DataRow dataRow)
    {
        ColumnMapping? columnMapping = null;
        if (columnMappings.TryGetValue("ProductManufacturerID", out columnMapping) && columnMapping.Active && columnMapping.DestinationColumn != null)
        {
            string? manufacturer = GetValue(columnMapping, row);
            if (!string.IsNullOrEmpty(manufacturer))
            {
                DataRow? manufacturerRow = null;
                string id;
                ProductManufacturers.TryGetValue(manufacturer, out manufacturerRow);
                if (manufacturerRow == null)
                {
                    manufacturerRow = GetExistingManufacturer(row, columnMapping);
                }
                if (manufacturerRow != null)
                {
                    id = Converter.ToString(manufacturerRow["ManufacturerID"]);
                }
                else
                {
                    manufacturerRow = FindRow("EcomManufacturers", manufacturer, dataRow.Table.Columns.Contains("ManufacturerName") ? new Func<DataRow, bool>(r => r["ManufacturerName"].ToString() == manufacturer) : null);
                    if (manufacturerRow != null)
                    {
                        id = Converter.ToString(manufacturerRow["ManufacturerID"]);
                    }
                    else
                    {
                        DataRow newManufacturer = GetDataTableNewRow("EcomManufacturers");
                        LastManufacturerId = LastManufacturerId + 1;
                        id = "ImportedMANU" + LastManufacturerId;
                        newManufacturer["ManufacturerID"] = id;
                        newManufacturer["ManufacturerName"] = manufacturer;
                        Dictionary<string, List<DataRow>>? manufacturers = null;
                        if (!DataRowsToWrite.TryGetValue(newManufacturer.Table.TableName, out manufacturers))
                        {
                            manufacturers = new Dictionary<string, List<DataRow>>();
                            DataRowsToWrite.Add("EcomManufacturers", manufacturers);
                        }
                        manufacturers.Add("ImportedMANU" + LastManufacturerId, new List<DataRow>() { newManufacturer });
                    }
                }

                dataRow[columnMapping.DestinationColumn.Name] = id;
                if (columnMapping.SourceColumn is not null)
                    row[columnMapping.SourceColumn.Name] = id;
            }
        }
    }

    private void HandleProductGroups(Dictionary<string, object> row, Dictionary<string, ColumnMapping> columnMappings, string productID, string productLanguageID, DataTable dataTable)
    {
        ColumnMapping? column = null;
        columnMappings.TryGetValue("Groups", out column);
        string? groups = GetValue(column, row);
        string group = string.Empty;
        if (!string.IsNullOrEmpty(groups))
        {
            try
            {
                ColumnMapping? sortingColumn = null;
                columnMappings.TryGetValue("GroupSorting", out sortingColumn);
                string groupSorting = Converter.ToString(GetValue(sortingColumn, row));

                ColumnMapping? primaryGroupColumn = null;
                columnMappings.TryGetValue("PrimaryGroup", out primaryGroupColumn);
                string primaryGroup = Converter.ToString(GetValue(primaryGroupColumn, row));

                var groupSortings = SplitOnComma(groupSorting);
                var groupIds = SplitOnComma(groups);

                List<string> missingGroups = new List<string>();
                for (int i = 0; i < groupIds.Length; i++)
                {
                    group = groupIds[i];
                    bool referenceAdded = false;
                    if (groupSortings.Length > i)
                    {
                        referenceAdded = AddGroupReferenceToProduct(productID, productLanguageID, group, int.Parse(groupSortings[i]), primaryGroup, dataTable);
                    }
                    else
                    {
                        referenceAdded = AddGroupReferenceToProduct(productID, productLanguageID, group, null, primaryGroup, dataTable);
                    }
                    if (!referenceAdded && !_createMissingGoups)
                    {
                        missingGroups.Add(group);
                    }
                }
                if (missingGroups.Count > 0)
                {
                    Dictionary<string, object> cloneRow = new Dictionary<string, object>(row);
                    cloneRow["Groups"] = string.Join(",", missingGroups.Distinct());
                    _rowsWithMissingGroups.Add(cloneRow);
                }
            }
            catch (Exception ex)
            {
                throw new Exception($"Handle product Groups failed. Group: '{group}'. Reason: {ex.Message}", ex);
            }
        }
    }

    private void HandleProductCategoryFields(Dictionary<string, object> row, Dictionary<string, ColumnMapping> columnMappings, string productID, string productVariantId, string productLanguageID)
    {
        var categoryFieldMappings = new List<ColumnMapping>();
        foreach (var columnMapping in columnMappings)
        {
            if (columnMapping.Key.StartsWith("ProductCategory|"))
            {
                categoryFieldMappings.Add(columnMapping.Value);
            }
        }

        if (categoryFieldMappings.Any())
        {
            try
            {
                foreach (var categoryFieldMapping in categoryFieldMappings)
                {
                    var categoryId = string.Empty;
                    var fieldId = string.Empty;
                    var fieldUniqueId = categoryFieldMapping.DestinationColumn.Name;
                    var tokens = fieldUniqueId.Split('|');

                    if (tokens.Count() == 3)
                    {
                        categoryId = tokens[1];
                        fieldId = tokens[2];

                        string? value = GetValue(categoryFieldMapping, row);
                        if (!_ignoreEmptyCategoryFieldValues || !string.IsNullOrEmpty(value))
                        {
                            AddCategoryFieldValueToProduct(productID, productVariantId, productLanguageID, categoryId, fieldId, value);
                        }
                    }
                }
            }
            catch (Exception ex)
            {
                throw new Exception("Write failed. Reason: " + ex.Message, ex);
            }
        }
    }

    private static string[] SplitOnComma(string? inputString)
    {
        return inputString is not null ? InternalSplitOnComma(inputString).ToArray() : new string[0];
    }

    private static IEnumerable<string> InternalSplitOnComma(string input)
    {
        var array = input.Split(new[] { ',' }, StringSplitOptions.RemoveEmptyEntries);
        for (var i = 0; i < array.Length; i++)
        {
            var split = array[i];
            yield return split.Trim('"');
        }
    }

    private string HandleVariantId(Dictionary<string, object> row, Dictionary<string, ColumnMapping> columnMappings, DataRow dataRow, DataRow? existingProductRow,
        ref string productId)
    {
        string productVariantID;
        columnMappings.TryGetValue("ProductVariantID", out ColumnMapping? column);
        if (column == null || column.SourceColumn == null || string.IsNullOrEmpty(Converter.ToString(row[column.SourceColumn.Name])))
        {
            productVariantID = existingProductRow != null ? existingProductRow["ProductVariantID"].ToString() ?? "" : "";
            if (column != null)
            {
                HandleProductIdFoundByNumber(row, columnMappings, dataRow, ref productId, ref productVariantID);
            }
            dataRow["ProductVariantID"] = productVariantID;
        }
        else
        {
            productVariantID = GetMergedValue(column, row) ?? "";
            HandleProductIdFoundByNumber(row, columnMappings, dataRow, ref productId, ref productVariantID);
        }

        return productVariantID;
    }

    private string HandleProductLanguageId(Dictionary<string, object> row, Dictionary<string, ColumnMapping> columnMappings, DataRow dataRow)
    {
        ColumnMapping? column = null;
        columnMappings.TryGetValue("ProductLanguageID", out column);
        string productLanguageID = _defaultLanguageId;
        if (column != null && column.Active)
        {
            if (column.HasScriptWithValue)
            {
                productLanguageID = GetLanguageID(column.GetScriptValue());
                dataRow["ProductLanguageID"] = productLanguageID;
            }
            else if (!column.HasScriptWithValue && column.SourceColumn != null && !string.IsNullOrEmpty(Converter.ToString(row[column.SourceColumn.Name])))
            {
                productLanguageID = GetLanguageID(row[column.SourceColumn.Name].ToString());
                row[column.SourceColumn.Name] = productLanguageID;
            }
        }
        else
        {
            dataRow["ProductLanguageID"] = productLanguageID;
            if (column?.SourceColumn != null)
            {
                row[column.SourceColumn.Name] = productLanguageID;
            }
        }

        return productLanguageID;
    }

    private string HandleProductId(Dictionary<string, object> row, Mapping mapping, Dictionary<string, ColumnMapping> columnMappings, DataRow dataRow, ref DataRow? existingProductRow, string productLanguageId)
    {
        string productID;
        ColumnMapping? productIdColumn = null;
        columnMappings.TryGetValue("ProductID", out productIdColumn);
        ColumnMapping? productNumberColumn = null;
        columnMappings.TryGetValue("ProductNumber", out productNumberColumn);
        ColumnMapping? productNameColumn = null;
        columnMappings.TryGetValue("ProductName", out productNameColumn);
        if (productIdColumn == null)
        {
            existingProductRow = GetExistingProduct(row, mapping, productNumberColumn, productNameColumn);
            if (existingProductRow == null)//if product is not found by number or name, return new generated ProductID                            
            {
                LastProductId = LastProductId + 1;
                productID = "ImportedPROD" + LastProductId;
            }
            else
            {
                productID = existingProductRow["ProductID"].ToString() ?? "";
            }
            dataRow["ProductID"] = productID;
            row["ProductID"] = productID;
        }
        else
        {
            productID = GetValue(productIdColumn, row) ?? "";
            if (string.IsNullOrEmpty(productID))
            {
                existingProductRow = GetExistingProduct(row, mapping, productNumberColumn, productNameColumn);
                if (existingProductRow == null)//if product is not found by number or name, return new generated ProductID                            
                {
                    LastProductId = LastProductId + 1;
                    productID = "ImportedPROD" + LastProductId;
                }
                else
                {
                    productID = existingProductRow["ProductID"].ToString() ?? "";
                }
                if (productIdColumn.SourceColumn is not null)
                    row[productIdColumn.SourceColumn.Name] = productID;
            }
        }
        if (productNumberColumn != null)
        {
            string? productNumber = GetValue(productNumberColumn, row);
            if (!string.IsNullOrEmpty(productNumber))
            {
                if (!ImportedProductsByNumber.ContainsKey(productNumber))
                {
                    ImportedProductsByNumber.Add(productNumber, dataRow);
                }
                if (!string.IsNullOrEmpty(productLanguageId))
                {
                    string key = GetImportedProductsByNumberMultipleProductsIdentifier(productNumber, productLanguageId);
                    if (!ImportedProductsByNumber.ContainsKey(key))
                    {
                        ImportedProductsByNumber.Add(key, dataRow);
                    }
                }
            }
        }
        return productID;
    }

    private bool WriteAssortments(Dictionary<string, object> row, Dictionary<string, ColumnMapping> columnMappings, DataRow dataRow, ColumnMappingCollection mappingColumns, Mapping mapping)
    {
        ColumnMapping? assortmentIdColumn = null;
        if (columnMappings.TryGetValue("AssortmentPermissionAssortmentID", out assortmentIdColumn))
        {
            string? assortmentID = GetValue(assortmentIdColumn, row);
            if (!string.IsNullOrEmpty(assortmentID))
            {
                List<string> userIDs = new List<string>();
                ColumnMapping? assortmentCustomerNumberColumn = null;
                if (columnMappings.TryGetValue("AssortmentPermissionCustomerNumber", out assortmentCustomerNumberColumn) && assortmentCustomerNumberColumn.Active)
                {
                    string? userNumber = GetValue(assortmentCustomerNumberColumn, row);
                    if (!string.IsNullOrEmpty(userNumber))
                    {
                        userIDs = ExistingUsers.Select("AccessUserCustomerNumber='" + userNumber.Replace("'", "''") + "'").Select(r => Converter.ToString(r["AccessUserID"])).ToList();
                    }
                }
                ColumnMapping? externalIdmapping = null;
                if (columnMappings.TryGetValue("AssortmentPermissionExternalID", out externalIdmapping) && externalIdmapping.Active)
                {
                    string? externalId = GetValue(externalIdmapping, row);
                    if (!string.IsNullOrEmpty(externalId))
                    {
                        userIDs.AddRange(ExistingUsers.Select("AccessUserExternalID='" + externalId.Replace("'", "''") + "'").Select(r => Converter.ToString(r["AccessUserID"])));
                    }
                }
                ColumnMapping? userIdMapping = null;
                if (columnMappings.TryGetValue("AssortmentPermissionAccessUserID", out userIdMapping) && userIdMapping.Active)
                {
                    string? id = GetValue(userIdMapping, row);
                    if (!string.IsNullOrEmpty(id))
                    {
                        userIDs.AddRange(ExistingUsers.Select("AccessUserID='" + id.Replace("'", "''") + "'").Select(r => Converter.ToString(r["AccessUserID"])));
                    }
                }
                userIDs = userIDs.Distinct().ToList();
                if (userIDs.Count > 0)
                {
                    if (userIDs.Count > 1)
                    {
                        if (duplicateRowsHandler is not null && duplicateRowsHandler.IsRowDuplicate(mappingColumns.Where(cm => cm.Active), mapping, dataRow, row))
                        {
                            return false;
                        }
                        var rows = new List<DataRow>();
                        foreach (string userID in userIDs)
                        {
                            DataRow relation = GetDataTableNewRow("EcomAssortmentPermissions");
                            relation["AssortmentPermissionAssortmentID"] = assortmentID;
                            relation["AssortmentPermissionAccessUserID"] = userID;
                            rows.Add(relation);
                        }
                        DataRowsToWrite[dataRow.Table.TableName].Add(RowAutoId++.ToString(), rows);
                    }
                    else
                    {
                        dataRow["AssortmentPermissionAssortmentID"] = assortmentID;
                        dataRow["AssortmentPermissionAccessUserID"] = userIDs[0];
                        return true;
                    }
                }
                return false;
            }
        }
        return true;
    }

    private void WriteDetails(Dictionary<string, object> row, Dictionary<string, ColumnMapping> columnMappings, DataRow dataRow)
    {
        ColumnMapping? detailIdColumn = null;
        if (!columnMappings.TryGetValue("DetailID", out detailIdColumn) || string.IsNullOrEmpty(GetValue(detailIdColumn, row)))
        {
            LastDetailId = LastDetailId + 1;
            dataRow["DetailID"] = "ImportedDETAIL" + LastDetailId;
        }
    }

    private void WritePrices(Dictionary<string, object> row, Dictionary<string, ColumnMapping> columnMappings, DataRow dataRow)
    {
        ColumnMapping? priceIdColumn = null;
        if (!columnMappings.TryGetValue("PriceID", out priceIdColumn) || string.IsNullOrEmpty(GetMergedValue(priceIdColumn, row)))
        {
            LastPriceId = LastPriceId + 1;
            dataRow["PriceID"] = "ImportedPRICE" + LastPriceId;
        }

        if (!columnMappings.TryGetValue("PriceCurrency", out var priceCurrencyColumn))
        {
            dataRow["PriceCurrency"] = Ecommerce.Services.Currencies.GetDefaultCurrency().Code;
        }
        else
        {
            var priceCurrencyValue = GetMergedValue(priceCurrencyColumn, row);
            if (string.IsNullOrWhiteSpace(priceCurrencyValue) && priceCurrencyColumn.SourceColumn is not null)
            {
                row[priceCurrencyColumn.SourceColumn.Name] = Ecommerce.Services.Currencies.GetDefaultCurrency().Code;
            }
        }

        bool foundMatchingGroup = false;
        if (columnMappings.TryGetValue("PriceAccessUserGroup", out var priceAccessUserGroupColumn))
        {
            var userGroupIdLookupValue = GetMergedValue(priceAccessUserGroupColumn, row);
            if (!string.IsNullOrWhiteSpace(userGroupIdLookupValue))
            {
                var userIDs = ExistingUserGroups.Select("AccessUserExternalID='" + userGroupIdLookupValue + "'").Select(r => Converter.ToString(r["AccessUserID"])).ToList();
                if (userIDs.Count == 0)
                {
                    userIDs = ExistingUserGroups.Select("AccessUserName='" + userGroupIdLookupValue + "'").Select(r => Converter.ToString(r["AccessUserID"])).ToList();
                }

                if (userIDs.Count > 0)
                {
                    dataRow["PriceUserGroupId"] = userIDs[0];
                    foundMatchingGroup = true;
                    if (userIDs.Count > 1)
                    {
                        logger.Warn($"EcomPrices: Found multiple UserGroups with AccessUserExternalId={userGroupIdLookupValue} using the first one with userId = {userIDs[0]}.");
                    }
                }
                else
                {
                    logger.Warn($"EcomPrices: Could not find any UserGroup with {userGroupIdLookupValue} as ExternalId.");
                }
            }
        }

        if (columnMappings.TryGetValue("PriceAccessUser", out var priceAccessUserColumn))
        {
            if (!foundMatchingGroup)
            {
                var userIdLookupValue = GetMergedValue(priceAccessUserColumn, row);
                if (!string.IsNullOrWhiteSpace(userIdLookupValue))
                {
                    var userIDs = new List<string>();

                    if (int.TryParse(userIdLookupValue, out _))
                    {
                        userIDs = ExistingUsers.Select("AccessUserID='" + userIdLookupValue + "'").Select(r => Converter.ToString(r["AccessUserID"])).ToList();
                    }
                    if (userIDs.Count == 0)
                    {
                        userIDs = ExistingUsers.Select("AccessUserExternalID='" + userIdLookupValue + "'").Select(r => Converter.ToString(r["AccessUserID"])).ToList();
                    }

                    if (userIDs.Count > 0)
                    {
                        dataRow["PriceUserId"] = userIDs[0];
                        if (userIDs.Count > 1)
                        {
                            logger.Warn($"EcomPrices: Found multiple Users with AccessUserExternalId={userIdLookupValue} using the first one with userId = {userIDs[0]}.");
                        }
                    }
                    else
                    {
                        logger.Warn($"EcomPrices: Could not find any User with {userIdLookupValue} as User ID or ExternalId.");
                    }
                }
            }
        }
    }

    private void WriteDiscounts(Dictionary<string, object> row, Dictionary<string, ColumnMapping> columnMappings, DataRow dataRow)
    {
        bool foundMatchingGroup = false;
        if (columnMappings.TryGetValue("DiscountAccessUserGroup", out var discountAccessUserGroupColumn))
        {
            var userGroupIdLookupValue = GetMergedValue(discountAccessUserGroupColumn, row);
            if (!string.IsNullOrWhiteSpace(userGroupIdLookupValue))
            {
                var userIDs = ExistingUserGroups.Select("AccessUserExternalID='" + userGroupIdLookupValue + "'").Select(r => Converter.ToString(r["AccessUserID"])).ToList();
                if (userIDs.Count > 0)
                {
                    dataRow["DiscountAccessUserGroupId"] = userIDs[0];
                    foundMatchingGroup = true;
                    if (userIDs.Count > 1)
                    {
                        logger.Warn($"Ecom Discounts: Found multiple UserGroups with AccessUserExternalId={userGroupIdLookupValue} using the first one with userId = {userIDs[0]}.");
                    }
                }
                else
                {
                    logger.Warn($"Ecom Discounts: Could not find any UserGroup with {userGroupIdLookupValue} as ExternalId.");
                }
            }
        }

        if (columnMappings.TryGetValue("DiscountAccessUser", out var discountAccessUserColumn))
        {
            if (!foundMatchingGroup)
            {
                var userIdLookupValue = GetMergedValue(discountAccessUserColumn, row);
                if (!string.IsNullOrWhiteSpace(userIdLookupValue))
                {
                    var userIDs = new List<string>();

                    if (int.TryParse(userIdLookupValue, out _))
                    {
                        userIDs = ExistingUsers.Select("AccessUserExternalID='" + userIdLookupValue + "'").Select(r => Converter.ToString(r["AccessUserID"])).ToList();
                    }
                    if (userIDs.Count > 0)
                    {
                        dataRow["DiscountAccessUserId"] = userIDs[0];
                        if (userIDs.Count > 1)
                        {
                            logger.Warn($"Ecom Discounts: Found multiple Users with AccessUserExternalId={userIdLookupValue} using the first one with userId = {userIDs[0]}.");
                        }
                    }
                    else
                    {
                        logger.Warn($"Ecom Discounts: Could not find any User with {userIdLookupValue} as ExternalId.");
                    }
                }
            }
        }

        if (!columnMappings.TryGetValue("DiscountCurrencyCode", out var discountCurrencyColumn))
        {
            dataRow["DiscountCurrencyCode"] = Ecommerce.Services.Currencies.GetDefaultCurrency().Code;
        }
        else
        {
            var discountCurrencyValue = GetMergedValue(discountCurrencyColumn, row);
            if (string.IsNullOrWhiteSpace(discountCurrencyValue) && discountCurrencyColumn.SourceColumn is not null)
            {
                row[discountCurrencyColumn.SourceColumn.Name] = Ecommerce.Services.Currencies.GetDefaultCurrency().Code;
            }
        }
    }

    private void WriteUnits(Dictionary<string, object> row, Dictionary<string, ColumnMapping> columnMappings, DataRow dataRow)
    {
        bool didFindUnit = false;
        if (columnMappings.TryGetValue("UnitId", out var unitIdColumn))
        {
            var unitIdLookupValue = GetMergedValue(unitIdColumn, row);
            if (!string.IsNullOrWhiteSpace(unitIdLookupValue))
            {
                var unitIDs = new List<string>();
                unitIDs = ExistingUnits.Select("UnitId='" + unitIdLookupValue + "'").Select(r => Converter.ToString(r["UnitId"])).ToList();
                if (unitIDs.Count > 0)
                {
                    dataRow["UnitId"] = unitIDs[0];
                    didFindUnit = true;
                }
            }
        }
        if (!didFindUnit && columnMappings.TryGetValue("UnitExternalId", out var unitExternalIdColumn))
        {
            var unitExternalIdLookupValue = GetMergedValue(unitExternalIdColumn, row);
            if (!string.IsNullOrWhiteSpace(unitExternalIdLookupValue))
            {
                var unitIDs = new List<string>();
                unitIDs = ExistingUnits.Select("UnitExternalId='" + unitExternalIdLookupValue + "'").Select(r => Converter.ToString(r["UnitExternalId"])).ToList();
                if (unitIDs.Count > 0)
                {
                    dataRow["UnitExternalId"] = unitIDs[0];
                }
            }
        }
    }

    private void WriteUnitTranslations(Dictionary<string, object> row, Dictionary<string, ColumnMapping> columnMappings, DataRow dataRow)
    {
        if (columnMappings.TryGetValue("UnitTranslationUnitId", out var unitTranslationUnitIdColumn))
        {
            var unitTranslationUnitIdLookupValue = GetMergedValue(unitTranslationUnitIdColumn, row);
            if (!string.IsNullOrWhiteSpace(unitTranslationUnitIdLookupValue))
            {
                var unitTranslationUnitIDs = new List<string>();
                unitTranslationUnitIDs = ExistingUnitTranslations.Select("UnitTranslationUnitId='" + unitTranslationUnitIdLookupValue + "'").Select(r => Converter.ToString(r["UnitTranslationUnitId"])).ToList();
                if (unitTranslationUnitIDs.Count > 0)
                {
                    dataRow["UnitTranslationUnitId"] = unitTranslationUnitIDs[0];
                }
            }
        }

        if (!columnMappings.TryGetValue("UnitTranslationLanguageId", out _))
        {
            dataRow["UnitTranslationLanguageId"] = _defaultLanguageId;
        }
    }

    private void WriteRelatedProducts(Dictionary<string, object> row, Dictionary<string, ColumnMapping> columnMappings, DataRow dataRow)
    {
        string relatedGroupLanguage = _defaultLanguageId;
        ColumnMapping? productRelatedLanguageIdColumn = null;
        if (columnMappings.TryGetValue("ProductRelatedLanguageID", out productRelatedLanguageIdColumn))
        {
            var value = GetValue(productRelatedLanguageIdColumn, row);
            if (!string.IsNullOrEmpty(value))
            {
                relatedGroupLanguage = GetLanguageID(value);
            }
        }

        string? relatedGroupID = null;
        bool relatedGroupIdIsConstant = false;
        ColumnMapping? productRelatedGroupIdMapping = null;
        if (columnMappings.TryGetValue("ProductRelatedGroupID", out productRelatedGroupIdMapping))
        {
            if (productRelatedGroupIdMapping.HasScriptWithValue)
            {
                relatedGroupID = productRelatedGroupIdMapping.ScriptValue;
                relatedGroupIdIsConstant = true;
            }
            else
            {
                relatedGroupID = Converter.ToString(row[productRelatedGroupIdMapping.SourceColumn.Name]);
            }
        }

        if (!relatedGroupIdIsConstant)
        {
            if (string.IsNullOrEmpty(relatedGroupID))
            {
                relatedGroupID = GetDefaultGroupID(relatedGroupLanguage);
            }
            else
            {
                var productsRelRow = FindRow("EcomProductsRelatedGroups", relatedGroupID.Replace("'", "''"));
                if (productsRelRow == null)
                {
                    List<DataRow>? productsRelRows = null;
                    if (ProductsRelatedGroups.TryGetValue(relatedGroupID.Replace("'", "''"), out productsRelRows))
                    {
                        productsRelRow = productsRelRows[0];
                    }
                }
                if (productsRelRow == null)
                {
                    productsRelRow = FindRow("EcomProductsRelatedGroups", relatedGroupID, new Func<DataRow, bool>(r => r["RelatedGroupName"].ToString() == relatedGroupID));
                }
                if (productsRelRow == null)
                {
                    relatedGroupID = CreateProductRelatedGroup(relatedGroupID, relatedGroupLanguage);
                }
                else
                {
                    relatedGroupID = productsRelRow["RelatedGroupID"].ToString();
                }
            }
        }
        dataRow["ProductRelatedGroupID"] = relatedGroupID;
    }

    private void WriteGroups(Dictionary<string, object> row, Dictionary<string, ColumnMapping> columnMappings, DataRow dataRow)
    {
        //Set ID if Missing
        string groupID = HandleGroupId(row, columnMappings, dataRow);

        //find Shops, create if missing, 
        HandleGroupShop(row, columnMappings, groupID);

        //find Shops, create if missing, 
        HandleParentGroups(row, columnMappings, groupID);

        string groupLanguageID = _defaultLanguageId;
        ColumnMapping? groupLanguageColumn = null;
        if (columnMappings.TryGetValue("GroupLanguageID", out groupLanguageColumn) && !groupLanguageColumn.HasScriptWithValue && !string.IsNullOrEmpty(Converter.ToString(row[groupLanguageColumn.SourceColumn.Name])))
        {
            groupLanguageID = GetLanguageID(row[groupLanguageColumn.SourceColumn.Name].ToString());
            row[groupLanguageColumn.SourceColumn.Name] = groupLanguageID;
        }
        else
        {
            dataRow["GroupLanguageID"] = groupLanguageID;
        }
    }

    private void HandleParentGroups(Dictionary<string, object> row, Dictionary<string, ColumnMapping> columnMappings, string groupID)
    {
        ColumnMapping? parentGroupsColumn = null;
        columnMappings.TryGetValue("ParentGroups", out parentGroupsColumn);
        string? parentGroups = GetValue(parentGroupsColumn, row);
        if (!string.IsNullOrEmpty(parentGroups))
        {
            var parentGroupIds = SplitOnComma(parentGroups);
            //getting GroupRelationsSorting values for parent groups
            List<int> parentGroupSortingList = new List<int>();
            if (isParentGroupSortingInEcomGroupsMapping)
            {
                ColumnMapping? parentGroupSortingColumn = null;
                columnMappings.TryGetValue("ParentGroupsSorting", out parentGroupSortingColumn);
                string? sortingStr = GetValue(parentGroupSortingColumn, row);
                if (!string.IsNullOrEmpty(sortingStr))
                {
                    var sortings = SplitOnComma(sortingStr);
                    for (int i = 0; i < sortings.Length; i++)
                    {
                        parentGroupSortingList.Add(int.Parse(sortings[i]));
                    }
                }
            }
            for (int i = 0; i < parentGroupIds.Length; i++)
            {
                string parentGroupId = parentGroupIds[i];
                int groupRelationsSorting = (parentGroupSortingList.Count > i) ? parentGroupSortingList[i] : 0;
                AddParentGroupReference(groupID, parentGroupId, groupRelationsSorting);
            }
        }
    }

    private void HandleGroupShop(Dictionary<string, object> row, Dictionary<string, ColumnMapping> columnMappings, string groupID)
    {
        ColumnMapping? groupShopsColumn = null;
        if (!columnMappings.TryGetValue("Shops", out groupShopsColumn))
        {
            AddShopReferenceToGroup(groupID, DefaultShop, 0);
        }
        else
        {
            bool useShopValueFromConstant = groupShopsColumn.ScriptTypeProvider.DisableSource && !string.IsNullOrEmpty(groupShopsColumn.ScriptTypeProvider.GetValue("")?.ToString());

            if (!useShopValueFromConstant && string.IsNullOrEmpty(Converter.ToString(row[groupShopsColumn.SourceColumn.Name])))
            {
                AddShopReferenceToGroup(groupID, DefaultShop, 0);
            }
            else
            {
                ColumnMapping? shopSortingColumn = null;
                columnMappings.TryGetValue("ShopSorting", out shopSortingColumn);
                string? shopSorting = GetValue(shopSortingColumn, row);
                shopSorting = string.IsNullOrEmpty(shopSorting) ? "0" : shopSorting;

                var sortings = SplitOnComma(shopSorting);
                string? shopIdsStr;
                if (useShopValueFromConstant)
                {
                    shopIdsStr = groupShopsColumn.ScriptTypeProvider.GetValue("")?.ToString();
                }
                else
                {
                    shopIdsStr = row[groupShopsColumn.SourceColumn.Name].ToString();
                }
                var shopIds = SplitOnComma(shopIdsStr);
                shopSorting = null;
                for (int i = 0; i < shopIds.Length; i++)
                {
                    if (sortings.Length > i)
                    {
                        shopSorting = sortings[i];
                    }

                    string shop = shopIds[i];

                    AddShopReferenceToGroupByShopName(groupID, shop, Converter.ToInt32(shopSorting));
                }
            }
        }
    }

    private string HandleGroupId(Dictionary<string, object> row, Dictionary<string, ColumnMapping> columnMappings, DataRow dataRow)
    {
        string? groupID;
        ColumnMapping? groupIdColumn = null;
        if (!columnMappings.TryGetValue("GroupID", out groupIdColumn))
        {
            LastGroupId = LastGroupId + 1;
            dataRow["GroupID"] = "ImportedGROUP" + LastGroupId;
            row["GroupID"] = "ImportedGROUP" + LastGroupId;
            groupID = "ImportedGROUP" + LastGroupId;
        }
        else
        {
            groupID = GetValue(groupIdColumn, row);
            if (string.IsNullOrEmpty(groupID))
            {
                LastGroupId = LastGroupId + 1;
                row["GroupID"] = "ImportedGROUP" + LastGroupId;
                groupID = "ImportedGROUP" + LastGroupId;
            }
        }
        return groupID;
    }

    private void WriteStockUnits(Dictionary<string, object> row, Dictionary<string, ColumnMapping> columnMappings, DataRow dataRow, Mapping mapping)
    {
        if (!columnMappings.TryGetValue("StockUnitId", out _))
        {
            if (columnMappings.TryGetValue("StockUnitProductID", out var stockUnitProductIDColumn) && columnMappings.TryGetValue("StockUnitVariantID", out var stockUnitVariantIDColumn))
            {
                mapping.AddMapping(mapping.SourceTable.Columns.FirstOrDefault(), mapping.DestinationTable.Columns.Where(obj => obj.Name.Equals("StockUnitId", StringComparison.OrdinalIgnoreCase)).FirstOrDefault());

                var productID = GetValue(stockUnitProductIDColumn, row);
                var variantID = GetValue(stockUnitVariantIDColumn, row);
                if (string.Equals(productID, variantID, StringComparison.OrdinalIgnoreCase))
                {
                    variantID = string.Empty;
                }

                var productBaseUnitOfMeasure = GetProductDefaultUnitId(productID ?? "", variantID ?? "");
                if (!string.IsNullOrEmpty(productBaseUnitOfMeasure))
                {
                    dataRow["StockUnitId"] = productBaseUnitOfMeasure;
                }
            }
        }
    }

    private void WriteCountries(Dictionary<string, object> row, Dictionary<string, ColumnMapping> columnMappings, DataRow dataRow, Mapping mapping)
    {
        if (columnMappings.TryGetValue("CountryCode2", out var countryCode2Column))
        {
            if (!columnMappings.TryGetValue("CountryCultureInfo", out _))
            {
                dataRow["CountryCultureInfo"] = "";
            }
        }
    }

    private void WriteStockLocations(Dictionary<string, object> row, Dictionary<string, ColumnMapping> columnMappings, DataRow dataRow)
    {
        if (!columnMappings.TryGetValue("StockLocationGroupId", out _))
        {
            StockLocation? existingStockLocation = null;
            if (columnMappings.TryGetValue("StockLocationId", out ColumnMapping? stockLocationIdentityColumn))
            {
                existingStockLocation = GetExistingStockLocation(row, stockLocationIdentityColumn);
            }
            else if (columnMappings.TryGetValue("StockLocationName", out stockLocationIdentityColumn))
            {
                existingStockLocation = GetExistingStockLocation(row, stockLocationIdentityColumn);
            }

            if (existingStockLocation != null)
            {
                dataRow["StockLocationGroupId"] = existingStockLocation.GroupID;
            }
            else
            {
                dataRow["StockLocationGroupId"] = GetLastStockLocationGroupId();
            }
        }
    }

    private string? GetProductDefaultUnitId(string productID, string variantID)
    {
        var product = Ecommerce.Services.Products.GetProductById(productID, variantID, true);
        if (product == null)
        {
            logger.Warn($"Could not find product with productid: {productID} and variantid:{variantID} on the default language");
        }
        return product?.DefaultUnitId;
    }

    private long GetStockLocationIdByName(Dictionary<string, object> row, ColumnMapping stockLocationIdColumn)
    {
        StockLocation? existingStockLocation = GetExistingStockLocation(row, stockLocationIdColumn);
        if (existingStockLocation != null)
        {
            return existingStockLocation.ID;
        }
        return 0;
    }

    private void WriteManufacturers(Dictionary<string, object> row, Dictionary<string, ColumnMapping> columnMappings, DataRow dataRow)
    {
        ColumnMapping? manufacturerNameColumn = null;
        columnMappings.TryGetValue("ManufacturerName", out manufacturerNameColumn);
        ColumnMapping? manufacturerColumn = null;
        if (!columnMappings.TryGetValue("ManufacturerID", out manufacturerColumn))
        {
            DataRow? existingManufacturer = GetExistingManufacturer(row, manufacturerNameColumn);
            if (existingManufacturer != null)
            {
                dataRow["ManufacturerID"] = existingManufacturer["ManufacturerID"];
            }
            else
            {
                LastManufacturerId = LastManufacturerId + 1;
                dataRow["ManufacturerID"] = "ImportedMANU" + LastManufacturerId;
            }
        }
        else if (string.IsNullOrEmpty(GetValue(manufacturerColumn, row)))
        {
            DataRow? existingManufacturer = GetExistingManufacturer(row, manufacturerNameColumn);
            if (existingManufacturer != null)
            {
                row["ManufacturerID"] = existingManufacturer["ManufacturerID"];
            }
            else
            {
                LastManufacturerId = LastManufacturerId + 1;
                row["ManufacturerID"] = "ImportedMANU" + LastManufacturerId;
            }
        }
    }

    private void WriteVariantOptions(Dictionary<string, object> row, Dictionary<string, ColumnMapping> columnMappings, DataRow dataRow)
    {
        HandleVariantOptionId(row, columnMappings, dataRow);

        HandleVariantOptionLangaugeId(row, columnMappings, dataRow);

        ColumnMapping? column = null;
        columnMappings.TryGetValue("VariantOptionGroupID", out column);
        string variantOptionGroupID = Converter.ToString(GetMergedValue(column, row));
        string variantOptionGroupIDEscaped = variantOptionGroupID.Replace("'", "''");

        DataRow? variantGroupRow = null;
        if (VariantGroups.TryGetValue(variantOptionGroupIDEscaped, out variantGroupRow))
        {
            dataRow["VariantOptionGroupID"] = variantGroupRow["VariantGroupID"];
            if (column?.SourceColumn is not null)
                row[column.SourceColumn.Name] = variantGroupRow["VariantGroupID"];
        }
        else
        {
            bool hasVariantGroupName = dataRow.Table.Columns.Contains("VariantGroupName");
            variantGroupRow = FindRow("EcomVariantGroups", variantOptionGroupIDEscaped, hasVariantGroupName ? new Func<DataRow, bool>(r => r["VariantGroupName"].ToString() == variantOptionGroupIDEscaped) : null);
            if (variantGroupRow != null)
            {
                dataRow["VariantOptionGroupID"] = variantGroupRow["VariantGroupID"];
                if (column?.SourceColumn is not null)
                    row[column.SourceColumn.Name] = variantGroupRow["VariantGroupID"];
            }
            else
            {
                AddNewVariantOptionGroup(row, columnMappings, column);
            }
        }
    }

    private void AddNewVariantOptionGroup(Dictionary<string, object> row, Dictionary<string, ColumnMapping> columnMappings, ColumnMapping? column)
    {
        var newGroup = GetDataTableNewRow("EcomVariantGroups");
        LastVariantGroupId = LastVariantGroupId + 1;
        //set groupID on option
        if (column?.SourceColumn is not null)
            newGroup["VariantGroupName"] = row[column.SourceColumn.Name];
        newGroup["VariantGroupLanguageID"] = _defaultLanguageId;
        if (newGroup.Table.Columns.Contains("VariantGroupFamily"))
        {
            newGroup["VariantGroupFamily"] = false;
        }
        string? variantGroupId;
        if (columnMappings.TryGetValue("VariantOptionId", out ColumnMapping? variantOptionIdColumn))
        {
            variantGroupId = GetMergedValue(variantOptionIdColumn, row);
        }
        else
        {
            variantGroupId = "ImportedVARGRP" + LastVariantGroupId;
        }
        newGroup["VariantGroupID"] = variantGroupId;
        DataRowsToWrite[newGroup.Table.TableName].TryAdd(variantGroupId ?? "", new List<DataRow>() { newGroup });
        row["VariantOptionGroupID"] = variantGroupId ?? "";
        if (column?.SourceColumn is not null)
            row[column.SourceColumn.Name] = newGroup["VariantGroupID"];
    }

    private void HandleVariantOptionLangaugeId(Dictionary<string, object> row, Dictionary<string, ColumnMapping> columnMappings, DataRow dataRow)
    {
        if (!columnMappings.TryGetValue("VariantOptionLanguageID", out ColumnMapping? column))
        {
            dataRow["VariantOptionLanguageID"] = _defaultLanguageId;
        }
        else
        {
            string? language = GetValue(column, row);
            if (string.IsNullOrEmpty(language))
            {
                row["VariantOptionLanguageID"] = _defaultLanguageId;
            }
            else
            {
                row["VariantOptionLanguageID"] = GetLanguageID(language);
            }
        }
    }

    private void HandleVariantOptionId(Dictionary<string, object> row, Dictionary<string, ColumnMapping> columnMappings, DataRow dataRow)
    {
        ColumnMapping? column = null;
        if (!columnMappings.TryGetValue("VariantOptionID", out column))
        {
            LastVariantOptionId = LastVariantOptionId + 1;
            dataRow["VariantOptionID"] = "ImportedVO" + LastVariantOptionId;
        }
        else
        {
            var value = GetValue(column, row);
            if (string.IsNullOrEmpty(value))
            {
                LastVariantOptionId = LastVariantOptionId + 1;
                row["VariantOptionID"] = "ImportedVO" + LastVariantOptionId;
            }
        }
    }

    private void WriteVariantGroups(Dictionary<string, object> row, Dictionary<string, ColumnMapping> columnMappings, DataRow dataRow)
    {
        ColumnMapping? variantGroupColumn = null;
        if (!columnMappings.TryGetValue("VariantGroupID", out variantGroupColumn))
        {
            LastVariantGroupId = LastVariantGroupId + 1;
            dataRow["VariantGroupID"] = "ImportedVARGRP" + LastVariantGroupId;
        }
        else
        {
            var value = GetValue(variantGroupColumn, row);
            if (string.IsNullOrEmpty(value))
            {
                LastVariantGroupId = LastVariantGroupId + 1;
                dataRow["VariantGroupID"] = "ImportedVARGRP" + LastVariantGroupId;
                if (variantGroupColumn.SourceColumn is not null)
                {
                    row[variantGroupColumn.SourceColumn.Name] = "ImportedVARGRP" + LastVariantGroupId;
                }
            }
        }

        string variantGroupLanguageID = _defaultLanguageId;
        ColumnMapping? variantGroupLanguageColumn = null;
        if (columnMappings.TryGetValue("VariantGroupLanguageID", out variantGroupLanguageColumn))
        {
            string? id = GetValue(variantGroupLanguageColumn, row);
            if (!string.IsNullOrEmpty(id))
            {
                variantGroupLanguageID = GetLanguageID(id);
            }
            if (variantGroupLanguageColumn.SourceColumn is not null)
            {
                row[variantGroupLanguageColumn.SourceColumn.Name] = variantGroupLanguageID;
            }
        }
        else
        {
            dataRow["VariantGroupLanguageID"] = variantGroupLanguageID;
        }
    }

    private void WriteCategoyFieldValues(Dictionary<string, object> row, Dictionary<string, ColumnMapping> columnMappings, DataRow dataRow)
    {
        DataRow? product = null;
        //Fill FieldValueFieldCategoryId by finding exisintg CategoryField by FieldID/SystemName
        ColumnMapping? fieldIdColumn = null;
        ColumnMapping? fieldCategoryIdColumn = null;
        if (columnMappings.TryGetValue("FieldValueFieldId", out fieldIdColumn) &&
            (!columnMappings.TryGetValue("FieldValueFieldCategoryId", out fieldCategoryIdColumn) || string.IsNullOrEmpty(GetValue(fieldCategoryIdColumn, row))))
        {
            var existingCategoryField = Ecommerce.Services.ProductCategories.GetCategories().SelectMany(c => Ecommerce.Services.ProductCategories.GetFieldsByCategoryId(c.Id)).FirstOrDefault(
                field => string.Compare(field.Id, GetValue(fieldIdColumn, row), true) == 0);
            if (existingCategoryField != null)
            {
                dataRow["FieldValueFieldCategoryId"] = existingCategoryField.Category != null ? existingCategoryField.Category.Id : "";
                if (fieldCategoryIdColumn?.SourceColumn != null)
                {
                    row[fieldCategoryIdColumn.SourceColumn.Name] = dataRow["FieldValueFieldCategoryId"];
                }
            }
        }

        ColumnMapping? fieldProductIdColumn = null;
        ColumnMapping? fieldProductNumberColumn = null;
        ColumnMapping? fieldProductLanguageIdColumn = null;
        ColumnMapping? fieldProductVariantIdColumn = null;

        columnMappings.TryGetValue("FieldValueProductLanguageId", out fieldProductLanguageIdColumn);
        columnMappings.TryGetValue("FieldValueProductVariantId", out fieldProductVariantIdColumn);

        if (!columnMappings.TryGetValue("FieldValueProductId", out fieldProductIdColumn) && columnMappings.TryGetValue("FieldValueProductNumber", out fieldProductNumberColumn))
        {
            //find id from productnumber, variantID and Language if available
            var productNumber = GetValue(fieldProductNumberColumn, row);
            string productLanguageId = _defaultLanguageId;
            if (fieldProductLanguageIdColumn != null && fieldProductLanguageIdColumn.Active && !fieldProductLanguageIdColumn.HasScriptWithValue &&
                !string.IsNullOrEmpty(GetValue(fieldProductLanguageIdColumn, row)))
            {
                productLanguageId = GetLanguageID(GetValue(fieldProductLanguageIdColumn, row));
            }
            if (!string.IsNullOrEmpty(productLanguageId) && !string.IsNullOrEmpty(productNumber))
            {
                string importedProductsByNumberIdentifier = GetImportedProductsByNumberMultipleProductsIdentifier(productNumber, productLanguageId);
                ImportedProductsByNumber.TryGetValue(importedProductsByNumberIdentifier, out product);
            }
            if (product == null && productNumber is not null)
            {
                ImportedProductsByNumber.TryGetValue(productNumber, out product);
            }
            if (product == null && productNumber is not null)
            {
                product = GetExistingProductForFieldValue(row, productNumber, fieldProductVariantIdColumn, fieldProductLanguageIdColumn);
            }

            if (product != null)
            {
                dataRow["FieldValueProductId"] = product["ProductId"];
                row["FieldValueProductId"] = dataRow["FieldValueProductId"];
            }
            else
            {
                throw new Exception(
                    "Attempt to import category field value failed: no productID found. Attempted to import Category field value:" +
                    dataRow.ToString());
            }
        }
        string fieldLanguageID = (product != null) ? product["ProductLanguageId"].ToString() ?? _defaultLanguageId : _defaultLanguageId;

        if (fieldProductLanguageIdColumn != null && fieldProductLanguageIdColumn.Active && !fieldProductLanguageIdColumn.HasScriptWithValue &&
            !string.IsNullOrEmpty(GetValue(fieldProductLanguageIdColumn, row)))
        {
            fieldLanguageID = GetLanguageID(GetValue(fieldProductLanguageIdColumn, row));
            if (fieldProductLanguageIdColumn.SourceColumn is not null)
                row[fieldProductLanguageIdColumn.SourceColumn.Name] = fieldLanguageID;
        }
        else
        {
            dataRow["FieldValueProductLanguageId"] = fieldLanguageID;
            if (fieldProductLanguageIdColumn != null && !fieldProductLanguageIdColumn.HasScriptWithValue)
            {
                row[fieldProductLanguageIdColumn.SourceColumn.Name] = fieldLanguageID;
            }
        }

        if (fieldProductVariantIdColumn == null || string.IsNullOrEmpty(GetValue(fieldProductVariantIdColumn, row)))
        {
            dataRow["FieldValueProductVariantId"] = product != null ? product["ProductVariantID"].ToString() : "";
            row["FieldValueProductVariantId"] = dataRow["FieldValueProductVariantId"];
        }
    }

    private DataRow? GetExistingProductForFieldValue(Dictionary<string, object> row, string productNumber, ColumnMapping? fieldProductVariantIdColumn, ColumnMapping? fieldProductLanguageIdColumn)
    {
        string selectExpression = "ProductNumber='" + productNumber + "'";

        if (fieldProductVariantIdColumn != null)
        {
            string productVariantId = GetValue(fieldProductVariantIdColumn, row) ?? "";
            selectExpression = selectExpression + " and ProductVariantId='" + productVariantId + "'";
        }

        if (fieldProductLanguageIdColumn != null)
        {
            string productLanguageId = GetValue(fieldProductLanguageIdColumn, row) ?? "";
            selectExpression = selectExpression + " and ProductLanguageId='" + productLanguageId + "'";
        }

        var rows = ExistingProducts.Select(selectExpression);
        if (rows.Length > 0)
        {
            return rows[0];
        }

        return null;
    }

    private void AddParentGroupReference(string groupId, string parentGroupId, int groupRelationsSorting)
    {
        groupId = groupId.Replace("'", "''");
        parentGroupId = parentGroupId.Replace("'", "''");
        var relationSeparator = "_;_";
        if (!DataRowsToWrite["EcomGroupRelations"].ContainsKey($"{groupId}{relationSeparator}{parentGroupId}"))
        {
            if (groupRelationsSorting == 0)//if no sorting value in the source and it is existing relation keep values from database
            {
                GroupRelationSorting.TryGetValue($"{groupId}{_sortingKeySeparator}{parentGroupId}", out groupRelationsSorting);
            }
            var newRow = DataToWrite?.Tables?["EcomGroupRelations"]?.NewRow();
            if (newRow is not null)
            {
                newRow["GroupRelationsGroupId"] = groupId;
                newRow["GroupRelationsParentId"] = parentGroupId;
                newRow["GroupRelationsSorting"] = groupRelationsSorting;
                DataRowsToWrite["EcomGroupRelations"].Add($"{groupId}{relationSeparator}{parentGroupId}", new List<DataRow>() { newRow });
            }
        }
    }

    private string GetLanguageID(string? languageId)
    {
        string result = _defaultLanguageId;
        languageId = languageId is null ? "" : languageId.Replace("'", "''");
        DataRow? languageRow = null;
        if (!EcomLanguages.TryGetValue(languageId, out languageRow))
        {
            foreach (var row in EcomLanguages.Values)
            {
                var languageRowCode = Converter.ToString(row["LanguageCode2"]);
                var languageRowName = Converter.ToString(row["LanguageName"]);
                if (string.Equals(languageRowCode, languageId, StringComparison.OrdinalIgnoreCase))
                {
                    languageRow = row;
                    break;
                }
                if (string.Equals(languageRowName, languageId, StringComparison.OrdinalIgnoreCase))
                {
                    languageRow = row;
                }
            }
        }

        if (languageRow == null)
        {
            if (!string.IsNullOrEmpty(languageId))
            {
                var row = FindRow("EcomLanguages", languageId);
                if (row == null)
                {
                    var filter = new Func<DataRow, bool>(r => r.Table.Columns.Contains("LanguageName") && r["LanguageName"].ToString() == languageId);
                    row = FindRow("EcomLanguages", filter);
                }
                //create new Language                    
                if (row == null)
                {
                    languageRow = GetDataTableNewRow("EcomLanguages");
                    LastLanguageId = LastLanguageId + 1;
                    result = "ImportedLANG" + LastLanguageId;
                    languageRow["LanguageID"] = result;
                    languageRow["LanguageName"] = languageId;
                    languageRow["LanguageNativeName"] = languageId;

                    DataRowsToWrite[languageRow.Table.TableName].Add("ImportedLANG" + LastLanguageId, new List<DataRow>() { languageRow });
                }
                else
                {
                    result = row["LanguageID"].ToString() ?? _defaultLanguageId;
                }
            }
        }
        else
        {
            result = languageRow["LanguageID"].ToString() ?? _defaultLanguageId;
        }
        return result;
    }

    private string? CreateProductRelatedGroup(string relatedGroupID, string relatedGroupLanguage)
    {
        DataRow? newProductRelatedGroup = DataToWrite?.Tables?["EcomProductsRelatedGroups"]?.NewRow();
        if (newProductRelatedGroup is null)
            return null;

        LastRelatedGroupID = LastRelatedGroupID + 1;
        string result = "ImportedRELGRP" + LastRelatedGroupID;
        newProductRelatedGroup["RelatedGroupID"] = result;
        newProductRelatedGroup["RelatedGroupName"] = relatedGroupID;
        newProductRelatedGroup["RelatedGroupLanguageID"] = relatedGroupLanguage;

        DataRowsToWrite["EcomProductsRelatedGroups"].Add("ImportedRELGRP" + LastRelatedGroupID, new List<DataRow>() { newProductRelatedGroup });

        return result;
    }

    private string? GetDefaultGroupID(string relatedGroupLanguage)
    {
        relatedGroupLanguage = relatedGroupLanguage.Replace("'", "''");
        var relationFound = false;
        foreach (var productRelGroupRows in ProductsRelatedGroups.Values)
        {
            foreach (var productRelGroupRow in productRelGroupRows)
            {
                var groupName = Converter.ToString(productRelGroupRow["RelatedGroupName"]);
                var languageId = Converter.ToString(productRelGroupRow["RelatedGroupLanguageID"]);
                if (groupName.Equals("Imported Relations Group") && languageId.Equals(relatedGroupLanguage))
                {
                    relationFound = true;
                    break;
                }
            }
        }
        if (!relationFound)
        {
            foreach (var productRelGroupRows in DataRowsToWrite["EcomProductsRelatedGroups"].Values)
            {
                foreach (var productRelGroupRow in productRelGroupRows)
                {
                    var groupName = Converter.ToString(productRelGroupRow["RelatedGroupName"]);
                    var groupLangauge = Converter.ToString(productRelGroupRow["RelatedGroupLanguageID"]);
                    if (string.Equals(groupName, "Imported Relations Group", StringComparison.OrdinalIgnoreCase) &&
                            string.Equals(groupLangauge, relatedGroupLanguage, StringComparison.OrdinalIgnoreCase))
                    {
                        return productRelGroupRow["RelatedGroupID"].ToString();
                    }
                }
            }
        }

        DataRow? newProductRelatedGroup = DataToWrite?.Tables?["EcomProductsRelatedGroups"]?.NewRow();
        if (newProductRelatedGroup is null)
            return null;

        LastRelatedGroupID = LastRelatedGroupID + 1;
        newProductRelatedGroup["RelatedGroupID"] = "ImportedRELGRP" + LastRelatedGroupID;
        newProductRelatedGroup["RelatedGroupName"] = "Imported Relations Group";
        newProductRelatedGroup["RelatedGroupLanguageID"] = relatedGroupLanguage;

        DataRowsToWrite["EcomProductsRelatedGroups"].Add("ImportedRELGRP" + LastRelatedGroupID, new List<DataRow>() { newProductRelatedGroup });
        return newProductRelatedGroup["RelatedGroupID"].ToString();

    }

    private void AddVariantGroupReferenceToProductByVariantGroupName(string productID, string variantGroupsString, DataTable dataTable)
    {
        variantGroupsString = variantGroupsString.Replace("'", "''");
        DataRow? variantGroupRow = null;
        if (VariantGroups.TryGetValue(variantGroupsString, out variantGroupRow))
        {
            AddVariantGroupReferenceToProduct(productID, variantGroupRow["VariantGroupID"].ToString() ?? "");
        }
        else
        {
            variantGroupRow = FindRow("EcomVariantGroups", variantGroupsString, dataTable.Columns.Contains("VariantGroupName") ? new Func<DataRow, bool>(r => r["VariantGroupName"].ToString() == variantGroupsString) : null);
            if (variantGroupRow != null)
            {
                AddVariantGroupReferenceToProduct(productID, variantGroupRow["VariantGroupID"].ToString() ?? "");
            }
            else
            {
                throw new Exception("Relation betweeen product \"" + productID + "\" and VariantGroup \"" + variantGroupsString + "\" can not be created. The variantgroup does not exist.");
            }
        }
    }

    private void AddShopReferenceToGroupByShopName(string groupID, string shopName, int shopSorting)
    {
        shopName = shopName.Replace("'", "''");
        DataRow? shopRow = null;
        foreach (var row in EcomShops.Values)
        {
            var name = Converter.ToString(row["shopName"]);
            var id = Converter.ToString(row["ShopID"]);
            if (id.Equals(shopName) || name.Equals(shopName))
            {
                shopRow = row;
            }
        }
        if (shopRow != null)
        {
            AddShopReferenceToGroup(groupID, shopRow["ShopID"].ToString() ?? "", shopSorting);
        }
        else
        {
            shopRow = FindRow("EcomShops", shopName);
            if (shopRow != null)
            {
                AddShopReferenceToGroup(groupID, shopRow["ShopID"].ToString() ?? "", shopSorting);
            }
            else
            {
                var newShop = DataToWrite?.Tables?["EcomShops"]?.NewRow();
                if (newShop is not null)
                {
                    newShop["ShopID"] = "ImportedSHOP" + LastShopId;
                    newShop["ShopName"] = shopName;
                    LastShopId = LastShopId + 1;
                    DataRowsToWrite["EcomProductsRelatedGroups"].Add("ImportedSHOP" + LastShopId, new List<DataRow>() { newShop });
                    AddShopReferenceToGroup(groupID, newShop["ShopID"].ToString() ?? "", shopSorting);
                }
            }
        }
    }
    /// <summary>
    /// Returns true if group reference created, otherwise false
    /// </summary>
    private bool AddGroupReferenceToProduct(string productID, string languageID, string group, int? sorting, string primaryGroup, DataTable dataTable)
    {
        bool result = true;
        bool isPrimary = (group == primaryGroup) ? true : false;
        group = group.Replace("'", "''");
        var filter = new Func<DataRow, bool>(g => g["GroupID"].ToString() == group || g["GroupName"].ToString() == group);
        List<DataRow> groupRows = FindExistingRows(ProductGroups, group, filter);
        if (groupRows != null && groupRows.Count > 0)
        {
            // Add product to all of the found existing groups
            foreach (DataRow row in groupRows)
            {
                AddGroupReferenceRowToProduct(productID, row["GroupID"].ToString() ?? "", sorting, isPrimary);
            }
        }
        else
        {
            DataRow? groupRow = FindRow("EcomGroups", group, new Func<DataRow, bool>(g => g["GroupName"].ToString() == group));
            if (groupRow != null)
            {
                AddGroupReferenceRowToProduct(productID, groupRow["GroupID"].ToString() ?? "", sorting, isPrimary);
            }
            else
            {
                if (_createMissingGoups)
                {
                    LastGroupId = LastGroupId + 1;
                    var newGroup = GetDataTableNewRow("EcomGroups");

                    newGroup["GroupID"] = "ImportedGROUP" + LastGroupId;
                    if (string.IsNullOrEmpty(languageID))
                    {
                        newGroup["GroupLanguageID"] = _defaultLanguageId;
                    }
                    else
                    {
                        newGroup["GroupLanguageID"] = languageID;
                    }
                    newGroup["GroupName"] = group;
                    DataRowsToWrite[newGroup.Table.TableName].Add("ImportedGROUP" + LastGroupId, new List<DataRow>() { newGroup });
                    AddShopReferenceToGroup("ImportedGROUP" + LastGroupId, DefaultShop, 0);
                    AddGroupReferenceRowToProduct(productID, newGroup["GroupID"].ToString() ?? "", sorting, isPrimary);
                }
                else
                {
                    result = false;
                }
            }
        }
        return result;
    }

    private List<DataRow> FindExistingRows(Dictionary<string, List<DataRow>> collection, string id, Func<DataRow, bool> filter)
    {
        List<DataRow>? rows = null;
        if (!collection.TryGetValue(id, out rows))
        {
            return collection.Values.SelectMany(g => g).Where(filter).ToList();
        }
        return rows;
    }

    private DataRow? FindRow(string tableName, Func<DataRow, bool> filter)
    {
        Dictionary<string, List<DataRow>>? collection = null;
        if (DataRowsToWrite.TryGetValue(tableName, out collection))
        {
            if (collection != null)
            {
                return collection.Values.SelectMany(v => v).FirstOrDefault(filter);
            }
        }
        else
        {
            foreach (var key in DataRowsToWrite.Keys)
            {
                if (key.StartsWith(tableName + "$", StringComparison.InvariantCultureIgnoreCase))
                {
                    collection = DataRowsToWrite[key];
                    if (collection != null)
                    {
                        DataRow? row = collection.Values.SelectMany(v => v).FirstOrDefault(filter);
                        if (row != null)
                        {
                            return row;
                        }
                    }
                }
            }
        }
        return null;
    }

    private DataRow? FindRow(string tableName, string idToLook, Func<DataRow, bool>? fallbackFilter = null)
    {
        DataRow? row = FindRow(tableName, idToLook);
        if (row == null && fallbackFilter != null)
        {
            row = FindRow(tableName, fallbackFilter);
        }
        return row;
    }

    private void AddCategoryFieldValueToProduct(string productID, string productVariantId, string languageID, string categoryId, string fieldId, string? value)
    {
        if (ProductCategoryFields.ContainsKey($"{categoryId}|{fieldId}"))
        {
            var separator = "_;_";
            var key = $"{fieldId}{separator}{categoryId}{separator}{productID}{separator}{productVariantId}{separator}{languageID}";
            if (!DataRowsToWrite["EcomProductCategoryFieldValue"].ContainsKey(key))
            {
                var categoryFieldValue = GetDataTableNewRow("EcomProductCategoryFieldValue");
                categoryFieldValue["FieldValueFieldId"] = fieldId;
                categoryFieldValue["FieldValueFieldCategoryId"] = categoryId;
                categoryFieldValue["FieldValueProductId"] = productID;
                categoryFieldValue["FieldValueProductVariantId"] = productVariantId;
                categoryFieldValue["FieldValueProductLanguageId"] = languageID;
                categoryFieldValue["FieldValueValue"] = value;

                DataRowsToWrite["EcomProductCategoryFieldValue"].Add(key, new List<DataRow>() { categoryFieldValue });
            }
        }
        else
        {
            //If you try to import values to a ProductCategoryField, which doesn't exist
        }
    }

    private void AddVariantGroupReferenceToProduct(string productID, string variantGroupID)
    {
        string key = string.Format("{0}.{1}", variantGroupID.ToLower(), productID.ToLower());
        if (!ecomVariantgroupProductrelationKeys.ContainsKey(key))
        {
            var variantGroupProductRelation = DataToWrite?.Tables?["EcomVariantgroupProductrelation"]?.NewRow();
            if (variantGroupProductRelation is not null)
            {
                variantGroupProductRelation["VariantGroupProductRelationVariantGroupID"] = variantGroupID;
                variantGroupProductRelation["VariantGroupProductRelationProductID"] = productID;
                _variantGroupProductRelationSortingCounter++;
                variantGroupProductRelation["VariantGroupProductRelationSorting"] = _variantGroupProductRelationSortingCounter;
                LastVariantGroupProductRelationID = LastVariantGroupProductRelationID + 1;
                variantGroupProductRelation["VariantgroupProductRelationID"] = "ImportedVARGRPPRODREL" + LastVariantGroupProductRelationID;

                DataRowsToWrite["EcomVariantgroupProductrelation"].Add(RowAutoId++.ToString(), new List<DataRow>() { variantGroupProductRelation });
                ecomVariantgroupProductrelationKeys.Add(key, null);
            }
        }
    }

    private void HandleVariantsCount(string productID, string variantOptionID)
    {
        int productVariantOptionsCount = 0;
        if (ProductVariantsCountDictionary.TryGetValue(productID, out productVariantOptionsCount))
        {
            ProductVariantsCountDictionary[productID] = productVariantOptionsCount + 1;
        }
        else
        {
            ProductVariantsCountDictionary[productID] = 1;
        }
        CountProductVariantGroups(productID, variantOptionID);
    }

    private void WriteVariantOptionRelation(string productID, string variantOptionID)
    {
        var variantOptionProductRelation = GetDataTableNewRow("EcomVariantOptionsProductRelation");
        variantOptionProductRelation["VariantOptionsProductRelationProductID"] = productID;
        variantOptionProductRelation["VariantOptionsProductRelationVariantID"] = variantOptionID;

        Dictionary<string, List<DataRow>>? variantOptionRelations;
        if (!DataRowsToWrite.TryGetValue(variantOptionProductRelation.Table.TableName, out variantOptionRelations))
        {
            variantOptionRelations = new Dictionary<string, List<DataRow>>();
            DataRowsToWrite.Add("EcomVariantOptionsProductRelation", variantOptionRelations);
        }
        variantOptionRelations.Add(RowAutoId++.ToString(), new List<DataRow>() { variantOptionProductRelation });
    }

    private void AddShopReferenceToGroup(string groupID, string shopID, int shopSorting)
    {
        var relationAdded = false;
        foreach (var relationList in DataRowsToWrite["EcomShopGroupRelation"].Values)
        {
            foreach (var relationRow in relationList)
            {
                var relationGroupId = Converter.ToString(relationRow["ShopGroupGroupID"]);
                var relationShopId = Converter.ToString(relationRow["ShopGroupShopID"]);
                if (string.Equals(relationGroupId, groupID, StringComparison.OrdinalIgnoreCase) &&
                    string.Equals(relationShopId, shopID, StringComparison.OrdinalIgnoreCase))
                {
                    relationAdded = true;
                    break;
                }
            }
            if (relationAdded)
            {
                break;
            }
        }
        if (!relationAdded)
        {
            if (shopSorting == 0)//if no sorting value for ShopGroup relation in the source and it is existing relation keep values from database
            {
                ShopGroupRelationSorting.TryGetValue($"{shopID}{_sortingKeySeparator}{groupID}", out shopSorting);
            }
            var shopGroupRelation = DataToWrite?.Tables?["EcomShopGroupRelation"]?.NewRow();
            if (shopGroupRelation is not null)
            {
                shopGroupRelation["ShopGroupShopID"] = shopID;
                shopGroupRelation["ShopGroupGroupID"] = groupID;
                shopGroupRelation["ShopGroupRelationsSorting"] = shopSorting;

                DataRowsToWrite["EcomShopGroupRelation"].Add(RowAutoId++.ToString(), new List<DataRow>() { shopGroupRelation });
            }
        }
    }

    private void AddGroupReferenceRowToProduct(string productID, string groupID, int? sorting, bool isPrimary)
    {
        string key = string.Format("{0}.{1}", groupID.ToLower(), productID.ToLower());
        if (!ecomGroupProductRelationKeys.ContainsKey(key))
        {
            bool process = true;
            Tuple<bool, int>? existingRelationIsPrimaryAndSorting;
            if (ExistingGroupProductRelations.TryGetValue($"{groupID}{_sortingKeySeparator}{productID}", out existingRelationIsPrimaryAndSorting) &&
                existingRelationIsPrimaryAndSorting.Item1 == isPrimary &&
                (!sorting.HasValue || sorting.Value == existingRelationIsPrimaryAndSorting.Item2))
            {
                process = false;
            }
            if (!sorting.HasValue)
            {
                sorting = existingRelationIsPrimaryAndSorting != null ? existingRelationIsPrimaryAndSorting.Item2 : 0;
            }
            if (process)
            {
                var groupProductRelation = DataToWrite?.Tables?["EcomGroupProductRelation"]?.NewRow();
                if (groupProductRelation is not null)
                {
                    groupProductRelation["GroupProductRelationGroupID"] = groupID;
                    groupProductRelation["GroupProductRelationProductID"] = productID;
                    groupProductRelation["GroupProductRelationSorting"] = sorting.Value;
                    groupProductRelation["GroupProductRelationIsPrimary"] = isPrimary;

                    DataRowsToWrite["EcomGroupProductRelation"].Add(RowAutoId++.ToString(), new List<DataRow>() { groupProductRelation });
                    ecomGroupProductRelationKeys.Add(key, null);
                }
            }

            if (isPrimary)
            {
                DataRow? existingPrimaryRow = null;
                if (PrimaryGroupProductRelations.TryGetValue(productID.Replace("'", "''"), out existingPrimaryRow))
                {
                    string existingPrimaryGroupID = existingPrimaryRow["GroupProductRelationGroupID"].ToString() ?? "";
                    key = string.Format("{0}.{1}", existingPrimaryGroupID.ToLower(), productID.ToLower());
                    if (existingPrimaryGroupID.ToLower() != groupID.ToLower() && !ecomGroupProductRelationKeys.ContainsKey(key))
                    {
                        //Update GroupProductRelationIsPrimary to false for the previous existing primary record
                        var groupProductRelation = DataToWrite?.Tables?["EcomGroupProductRelation"]?.NewRow();
                        if (groupProductRelation is not null)
                        {
                            groupProductRelation["GroupProductRelationGroupID"] = existingPrimaryRow["GroupProductRelationGroupID"];
                            groupProductRelation["GroupProductRelationProductID"] = existingPrimaryRow["GroupProductRelationProductID"];
                            groupProductRelation["GroupProductRelationSorting"] = existingPrimaryRow["GroupProductRelationSorting"];
                            groupProductRelation["GroupProductRelationIsPrimary"] = false;

                            DataRowsToWrite["EcomGroupProductRelation"].Add(RowAutoId++.ToString(), new List<DataRow>() { groupProductRelation });
                            ecomGroupProductRelationKeys.Add(key, null);
                        }
                    }
                }
            }
        }
    }

    protected internal string? _defaultShop;
    private bool _removeFromEcomGroups;
    private bool _removeFromEcomVariantGroups;
    private readonly bool deleteExcess;

    protected internal string DefaultShop
    {
        get
        {
            if (string.IsNullOrEmpty(_defaultShop))
            {
                sqlCommand.CommandText = "select top(1) ShopID from EcomShops order by ShopDefault DESC, shopID";
                var result = sqlCommand.ExecuteReader();
                result.Read();
                _defaultShop = result["ShopID"].ToString() ?? "";
                result.Close();
            }
            return _defaultShop;
        }
        set { _defaultShop = value; }
    }

    public void ReportProgress(Mapping mapping)
    {
        if (mapping.DestinationTable != null)
        {
            var tableName = GetTableName(mapping.DestinationTable.Name, mapping);
            if (_writtenRowsCount % 10000 != 0)
            {
                logger.Log("Added " + _writtenRowsCount + " rows to temporary table for " + mapping.DestinationTable.Name + ".");
            }
        }
    }

    public void FinishWriting()
    {
        foreach (DataTable table in DataToWrite.Tables)
        {
            Dictionary<string, List<DataRow>>? tableRows = null;
            if (DataRowsToWrite.TryGetValue(table.TableName, out tableRows))
            {
                table.Rows.Clear();
                foreach (var rows in tableRows.Values)
                {
                    foreach (var tableRow in rows)
                    {
                        table.Rows.Add(tableRow);
                    }
                }
            }
            using (SqlBulkCopy sqlBulkCopier = new SqlBulkCopy(connection))
            {
                sqlBulkCopier.DestinationTableName = GetTableNameWithoutPrefix(table.TableName) + TempTableName + GetPrefixFromTableName(table.TableName);
                sqlBulkCopier.BulkCopyTimeout = 0;
                int skippedFailedRowsCount = 0;
                try
                {
                    sqlBulkCopier.WriteToServer(table);
                }
                catch
                {
                    string errors = BulkCopyHelper.GetBulkCopyFailures(sqlBulkCopier, table);
                    if (_skipFailingRows)
                    {
                        skippedFailedRowsCount = errors.Split(new string[] { System.Environment.NewLine }, StringSplitOptions.RemoveEmptyEntries).Length - 1;
                        skippedFailedRowsCount = skippedFailedRowsCount < 0 ? 0 : skippedFailedRowsCount;
                        if (skippedFailedRowsCount > 0)
                        {
                            logger.Log($"Skipped {skippedFailedRowsCount} failing rows from the temporary {GetTableNameWithoutPrefix(table.TableName)} table");
                        }
                    }
                    else
                    {
                        throw new Exception(errors);
                    }
                }
                RowsToWriteCount += (table.Rows.Count - skippedFailedRowsCount);
            }
        }
    }

    public int DeleteExcessFromMainTable(string shop, SqlTransaction transaction, Dictionary<string, Mapping> mappings)
    {
        sqlCommand.Transaction = transaction;
        var mapping = mappings.Values.FirstOrDefault();
        if (mapping is null)
            return 0;

        string extraConditions = GetExtraConditions(mapping, shop, null);
        return DeleteRowsFromMainTable(false, mappings, extraConditions, sqlCommand);
    }

    public void DeleteExcessFromMainTable(string shop, SqlTransaction transaction, string languageId, bool deleteProductsAndGroupForSpecificLanguage, bool hideDeactivatedProducts)
    {
        foreach (Mapping mapping in job.Mappings.Where(m => !_addedMappingsForMoveToMainTables.Contains(m)))
        {
            string tempTablePrefix = TempTableName + mapping.GetId();
            if (HasRowsToImport(mapping, out tempTablePrefix))
            {
                if ((mapping.DestinationTable.Name == "EcomProducts" || mapping.DestinationTable.Name == "EcomGroups") && deleteProductsAndGroupForSpecificLanguage)
                {
                    sqlCommand.Transaction = transaction;
                    string extraConditions = GetDeleteFromSpecificLanguageExtraCondition(mapping, tempTablePrefix, languageId);
                    var rowsAffected = DeleteExcessFromMainTable(sqlCommand, mapping, extraConditions, tempTablePrefix, removeMissingAfterImportDestinationTablesOnly);
                    if (rowsAffected > 0)
                    {
                        logger.Log($"The number of deleted rows: {rowsAffected} for the destination {mapping.DestinationTable.Name} table mapping");
                        RowsAffected += (int)rowsAffected;
                    }
                }
                else if (!(mapping.DestinationTable.Name == "EcomGroups" && !_removeFromEcomGroups) && !(mapping.DestinationTable.Name == "EcomVariantGroups" && !_removeFromEcomVariantGroups))
                {
                    bool? optionValue = mapping.GetOptionValue("RemoveMissingAfterImport");
                    bool removeMissingAfterImport = optionValue.HasValue ? optionValue.Value : deleteExcess;
                    optionValue = mapping.GetOptionValue("DeactivateMissingProducts");
                    bool deactivateMissing = optionValue.HasValue ? optionValue.Value : deactivateMissingProducts;

                    sqlCommand.Transaction = transaction;
                    if (mapping.DestinationTable.Name == "EcomProducts" && deactivateMissing)
                    {
                        var rowsAffected = DeactivateMissingProductsInMainTable(mapping, sqlCommand, shop, _defaultLanguageId, hideDeactivatedProducts);
                        if (rowsAffected > 0)
                        {
                            logger.Log($"The number of the deactivated product rows: {rowsAffected}");
                            RowsAffected += rowsAffected;
                        }
                    }
                    else if (removeMissingAfterImport || removeMissingAfterImportDestinationTablesOnly)
                    {
                        var rowsAffected = DeleteExcessFromMainTable(sqlCommand, mapping, GetExtraConditions(mapping, shop, null), tempTablePrefix, removeMissingAfterImportDestinationTablesOnly);
                        if (rowsAffected > 0)
                        {
                            logger.Log($"The number of deleted rows: {rowsAffected} for the destination {mapping.DestinationTable.Name} table mapping");
                            RowsAffected += (int)rowsAffected;
                        }
                    }
                }
            }
        }
    }

    public void DeleteExistingFromMainTable(string shop, SqlTransaction transaction, string languageId)
    {
        sqlCommand.Transaction = transaction;
        foreach (Mapping mapping in job.Mappings)
        {
            string tempTablePrefix = TempTableName + mapping.GetId();
            if (HasRowsToImport(mapping, out tempTablePrefix))
            {
                var rowsAffected = DeleteExistingFromMainTable(sqlCommand, mapping, GetExtraConditions(mapping, shop, languageId), tempTablePrefix);
                if (rowsAffected > 0)
                {
                    logger.Log($"The number of deleted rows: {rowsAffected} for the destination {mapping.DestinationTable.Name} table mapping");
                    RowsAffected += rowsAffected;
                }
            }
        }
    }

    internal static string GetDeleteFromSpecificLanguageExtraCondition(Mapping mapping, string tempTablePrefix, string languageId)
    {
        string ret = string.Empty;
        if (mapping != null && mapping.DestinationTable != null)
        {
            if (mapping.DestinationTable.Name == "EcomProducts")
            {
                if (!string.IsNullOrEmpty(languageId))
                {
                    ret = string.Format(" AND [EcomProducts].[ProductLanguageID] = '{0}' ", languageId);
                }
                else
                {
                    ret = string.Format(" AND [EcomProducts].[ProductLanguageID] IN (SELECT DISTINCT([ProductLanguageID]) FROM [EcomProducts{0}]) ", tempTablePrefix);
                }
            }
            else if (mapping.DestinationTable.Name == "EcomGroups")
            {
                if (!string.IsNullOrEmpty(languageId))
                {
                    ret = string.Format(" AND [EcomGroups].[GroupLanguageID] = '{0}' ", languageId);
                }
                else
                {
                    ret = string.Format(" AND [EcomGroups].[GroupLanguageID] IN (SELECT DISTINCT([GroupLanguageID]) FROM [EcomGroups{0}]) ", tempTablePrefix);
                }
            }
        }
        return ret;
    }

    public void MoveDataToMainTables(string shop, SqlTransaction sqlTransaction, bool updateOnly, bool insertOnly)
    {
        bool isGroupsColumnInMapping = false;
        Mapping? productsMapping = null;
        if (Mappings.TryGetValue("EcomProducts", out List<Mapping>? productsMappings))
        {
            productsMapping = productsMappings.FirstOrDefault();
        }
        if (productsMapping != null)
        {
            isGroupsColumnInMapping = productsMapping.GetColumnMappings(true).Any(cm => cm != null && cm.DestinationColumn != null && cm.Active && string.Compare(cm.DestinationColumn.Name, "Groups", true) == 0);
        }

        //Add mappings that are missing but needs to be there - EcomGroups & EcomVariantGroups.
        AddMappingsToJobThatNeedsToBeThereForMoveToMainTables();
        //Remove column mappings that shouldn't be included in move
        RemoveColumnMappingsFromJobThatShouldBeSkippedInMoveToMainTables();

        //Do Move for each mapped table
        foreach (Mapping mapping in job.Mappings)
        {
            var mappingId = mapping.GetId();
            if (mapping.Active && ColumnMappingsByMappingId[mappingId].Count > 0)
            {
                string tempTablePrefix = TempTableName + mappingId;
                if (HasRowsToImport(mapping, out tempTablePrefix))
                {
                    bool? optionValue = mapping.GetOptionValue("UpdateOnlyExistingRecords");
                    bool updateOnlyExistingRecords = optionValue.HasValue ? optionValue.Value : updateOnly;

                    MoveDataToMainTable(mapping, tempTablePrefix, sqlTransaction, updateOnlyExistingRecords, insertOnly);
                }
                else
                {
                    logger.Log(string.Format("No rows were imported to the table: {0}.", mapping.DestinationTable.Name));
                }
            }
        }

        RemoveExcessFromRelationsTables(sqlTransaction);

        string? groupProductRelationTempTablePrefix = null;
        bool removeMissingAfterImport = deleteExcess;
        if (productsMapping != null)
        {
            bool? optionValue = productsMapping.GetOptionValue("RemoveMissingAfterImport");
            removeMissingAfterImport = optionValue.HasValue ? optionValue.Value : deleteExcess;
        }

        if (((removeMissingAfterImport && isGroupsColumnInMapping) || (partialUpdate && !removeMissingAfterImportDestinationTablesOnly)) && HasRowsToImport(productsMapping, out groupProductRelationTempTablePrefix))
        {
            DeleteExcessFromGroupProductRelation(shop, sqlTransaction);
        }
    }

    private void MoveDataToMainTable(Mapping mapping, string tempTablePrefix, SqlTransaction sqlTransaction, bool updateOnly, bool insertOnly)
    {
        sqlCommand.Transaction = sqlTransaction;
        List<string> insertColumns = new List<string>();
        try
        {
            string sqlConditions = "";
            string firstKey = "";
            var columnMappings = new ColumnMappingCollection(mapping.GetColumnMappings().Where(m => m.Active).DistinctBy(obj => obj.DestinationColumn.Name));
            foreach (ColumnMapping columnMapping in columnMappings)
            {
                if (columnMapping.Active)
                {
                    SqlColumn column = (SqlColumn)columnMapping.DestinationColumn;
                    if (column.IsKeyColumn(columnMappings))
                    {
                        sqlConditions = sqlConditions + "[" + mapping.DestinationTable.SqlSchema + "].[" +
                                              mapping.DestinationTable.Name + "].[" + columnMapping.DestinationColumn.Name + "]=[" +
                                              mapping.DestinationTable.SqlSchema + "].[" +
                                              mapping.DestinationTable.Name + tempTablePrefix + "].[" + columnMapping.DestinationColumn.Name + "] and ";
                        if (firstKey == "")
                        {
                            firstKey = columnMapping.DestinationColumn.Name;
                        }
                    }
                }
            }
            sqlConditions = sqlConditions.Substring(0, sqlConditions.Length - 4);
            string insertSelect = "";
            string updateColumns = "";
            foreach (var columnMapping in columnMappings)
            {
                if (columnMapping.Active)
                {
                    if (mapping.DestinationTable.Name.Equals("EcomProducts", StringComparison.OrdinalIgnoreCase) &&
                            (columnMapping.DestinationColumn.Name.Equals("ProductUpdated", StringComparison.OrdinalIgnoreCase) ||
                                columnMapping.DestinationColumn.Name.Equals("ProductCreated", StringComparison.OrdinalIgnoreCase)))
                    {
                        continue;
                    }
                    insertColumns.Add("[" + columnMapping.DestinationColumn.Name + "]");
                    insertSelect = insertSelect + "[dbo].[" + columnMapping.DestinationColumn.Table.Name + tempTablePrefix + "].[" + columnMapping.DestinationColumn.Name + "], ";

                    if (!((SqlColumn)columnMapping.DestinationColumn).IsIdentity && !((SqlColumn)columnMapping.DestinationColumn).IsKeyColumn(columnMappings) && !columnMapping.ScriptValueForInsert)
                    {
                        updateColumns = updateColumns + "[" + columnMapping.DestinationColumn.Name + "]=[" + mapping.DestinationTable.SqlSchema + "].[" + columnMapping.DestinationColumn.Table.Name + tempTablePrefix + "].[" + columnMapping.DestinationColumn.Name + "], ";
                    }
                }
            }
            string sqlUpdateInsert = "";
            if (!string.IsNullOrEmpty(updateColumns) && !insertOnly)
            {
                if (mapping.DestinationTable.Name.Equals("EcomProducts", StringComparison.OrdinalIgnoreCase))
                {
                    updateColumns += $"[ProductUpdated] = '{DateTime.Now.ToString("yyyy-MM-dd HH:mm:ss", System.Globalization.CultureInfo.InvariantCulture)}' ";
                }
                else
                {
                    updateColumns = updateColumns.Substring(0, updateColumns.Length - 2);
                }
                sqlUpdateInsert = sqlUpdateInsert + "update [" + mapping.DestinationTable.SqlSchema + "].[" + mapping.DestinationTable.Name + "] set " + updateColumns + " from [" + mapping.DestinationTable.SqlSchema + "].[" + mapping.DestinationTable.Name + tempTablePrefix + "] where " + sqlConditions + ";";
            }
            if (!string.IsNullOrEmpty(insertSelect))
            {
                if (mapping.DestinationTable.Name.Equals("EcomProducts", StringComparison.OrdinalIgnoreCase))
                {
                    insertColumns.Add("[ProductCreated]");
                    insertColumns.Add("[ProductUpdated]");
                    insertSelect += $"'{DateTime.Now.ToString("yyyy-MM-dd HH:mm:ss", System.Globalization.CultureInfo.InvariantCulture)}', ";
                    insertSelect += $"'{DateTime.Now.ToString("yyyy-MM-dd HH:mm:ss", System.Globalization.CultureInfo.InvariantCulture)}' ";
                }
                else
                {
                    insertSelect = insertSelect.Substring(0, insertSelect.Length - 2);
                }
                if (!updateOnly)
                {
                    if (HasIdentity(columnMappings))
                    {
                        sqlUpdateInsert = sqlUpdateInsert + "set identity_insert [" + mapping.DestinationTable.SqlSchema + "].[" +
                                             mapping.DestinationTable.Name + "] ON;";
                    }
                    if (mapping.DestinationTable.Name != "EcomProducts" || (mapping.DestinationTable.Name == "EcomProducts" && !updateOnlyExistingProducts))
                    {
                        sqlUpdateInsert = sqlUpdateInsert + " insert into [" + mapping.DestinationTable.SqlSchema + "].[" + mapping.DestinationTable.Name + "] (" + string.Join(",", insertColumns) + ") (" +
                        "select " + insertSelect + " from [" + mapping.DestinationTable.SqlSchema + "].[" + mapping.DestinationTable.Name + tempTablePrefix + "] left outer join [" + mapping.DestinationTable.SqlSchema + "].[" + mapping.DestinationTable.Name + "] on " + sqlConditions + " where [" + mapping.DestinationTable.SqlSchema + "].[" + mapping.DestinationTable.Name + "].[" + firstKey + "] is null);";
                        if (HasIdentity(columnMappings))
                        {
                            sqlUpdateInsert = sqlUpdateInsert + "set identity_insert [" + mapping.DestinationTable.SqlSchema + "].[" +
                                                mapping.DestinationTable.Name + "] OFF;";
                        }
                    }
                }
            }
            sqlCommand.CommandText = sqlUpdateInsert;

            int timeout = SystemConfiguration.Instance.GetInt32("/GlobalSettings/System/Database/CommandTimeout");
            if (SystemConfiguration.Instance.GetInt32("/Globalsettings/DataIntegration/SQLSourceCommandTimeout") > timeout)
                timeout = SystemConfiguration.Instance.GetInt32("/Globalsettings/DataIntegration/SQLSourceCommandTimeout");
            if (timeout < 360)
                timeout = 360;
            sqlCommand.CommandTimeout = timeout;
            var rowsAffected = sqlCommand.ExecuteNonQuery();
            if (rowsAffected > 0)
            {
                logger.Log($"The number of rows affected: {rowsAffected} in the {mapping.DestinationTable.Name} table");
            }
            RowsAffected += rowsAffected;
        }
        catch (Exception ex)
        {
            throw GetMoveDataToMainTableException(ex, sqlCommand, mapping, tempTablePrefix, mapping.GetColumnMappings().Where(cm => cm.Active).Select(cm => "[" + cm.DestinationColumn.Name + "]").ToList());
        }
    }

    /// <remarks>
    ///  Source columns are irrelevant, but must be set, so they are set to a random column
    /// </remarks>
    private void AddMappingsToJobThatNeedsToBeThereForMoveToMainTables()
    {
        var schemaTables = job.Destination.GetSchema().GetTables();
        var tableColumnsDictionary = new Dictionary<string, Dictionary<string, Column>>();
        foreach (var table in schemaTables)
        {
            var columnsDictionary = new Dictionary<string, Column>(StringComparer.OrdinalIgnoreCase);
            tableColumnsDictionary.Add(table.Name, columnsDictionary);
            foreach (var column in table.Columns)
            {
                if (!columnsDictionary.ContainsKey(column.Name))
                {
                    columnsDictionary.Add(column.Name, column);
                }
            }
        }

        if (HasData("EcomGroups"))
        {
            if (!Mappings.ContainsKey("EcomGroups"))
            {
                Mapping ecomGroupsMapping = job.AddMapping();
                EnsureMapping(ecomGroupsMapping, null, tableColumnsDictionary["EcomGroups"],
                    new string[] { "GroupID", "GroupLanguageID", "GroupName" });
                job.Mappings.Add(ecomGroupsMapping);
                _addedMappingsForMoveToMainTables.Add(ecomGroupsMapping);
                _removeFromEcomGroups = false;
                ColumnMappingsByMappingId.Add(ecomGroupsMapping.GetId(), ecomGroupsMapping.GetColumnMappings(true));
            }
            else
            {
                foreach (Mapping groupMapping in Mappings["EcomGroups"])
                {
                    EnsureMapping(groupMapping, DestinationColumnMappings["EcomGroups"], tableColumnsDictionary["EcomGroups"],
                        new string[] { "GroupID", "GroupLanguageID" });
                }
            }
        }

        if (HasData("EcomProductCategoryFieldValue"))
        {
            if (!Mappings.ContainsKey("EcomProductCategoryFieldValue"))
            {
                Mapping ecomCategoryFieldValueMapping = job.AddMapping();
                EnsureMapping(ecomCategoryFieldValueMapping, null, tableColumnsDictionary["EcomProductCategoryFieldValue"],
                    new string[] { "FieldValueFieldId", "FieldValueFieldCategoryId", "FieldValueProductId", "FieldValueProductVariantId", "FieldValueProductLanguageId", "FieldValueValue" });
                job.Mappings.Add(ecomCategoryFieldValueMapping);
                _addedMappingsForMoveToMainTables.Add(ecomCategoryFieldValueMapping);
                ColumnMappingsByMappingId.Add(ecomCategoryFieldValueMapping.GetId(), ecomCategoryFieldValueMapping.GetColumnMappings(true));
            }
        }

        if (HasData("EcomVariantGroups"))
        {
            if (!Mappings.ContainsKey("EcomVariantGroups"))
            {
                Mapping ecomVariantsGroupsMapping = job.AddMapping();
                EnsureMapping(ecomVariantsGroupsMapping, null, tableColumnsDictionary["EcomVariantGroups"],
                    new string[] { "VariantGroupID", "VariantGroupLanguageID", "VariantGroupName" });
                job.Mappings.Add(ecomVariantsGroupsMapping);
                _addedMappingsForMoveToMainTables.Add(ecomVariantsGroupsMapping);
                _removeFromEcomVariantGroups = false;
                ColumnMappingsByMappingId.Add(ecomVariantsGroupsMapping.GetId(), ecomVariantsGroupsMapping.GetColumnMappings(true));
            }
            else
            {
                foreach (var ecomVariantsGroupsMapping in Mappings["EcomVariantGroups"])
                {
                    EnsureMapping(ecomVariantsGroupsMapping, DestinationColumnMappings["EcomVariantGroups"], tableColumnsDictionary["EcomVariantGroups"],
                        new string[] { "VariantGroupID", "VariantGroupLanguageID" });
                }
            }
        }

        if (HasData("EcomVariantsOptions") && job.Mappings.Find(m => m.DestinationTable.Name == "EcomVariantsOptions") != null)
        {
            foreach (var ecomVariantsOptionsMapping in job.Mappings.FindAll(m => m.DestinationTable.Name == "EcomVariantsOptions"))
            {
                EnsureMapping(ecomVariantsOptionsMapping, DestinationColumnMappings["EcomVariantsOptions"], tableColumnsDictionary["EcomVariantsOptions"],
                    new string[] { "VariantOptionID", "VariantOptionLanguageID" });
            }
        }

        List<Mapping>? productsMappings = null;
        if (HasData("EcomProducts") && Mappings.TryGetValue("EcomProducts", out productsMappings))
        {
            if (!updateOnlyExistingProducts)
            {
                foreach (var ecomProductsMapping in productsMappings)
                {
                    EnsureMapping(ecomProductsMapping, DestinationColumnMappings["EcomProducts"], tableColumnsDictionary["EcomProducts"],
                        new string[] { "ProductID", "ProductVariantID", "ProductLanguageID" });
                    EnsureMapping(ecomProductsMapping, DestinationColumnMappings["EcomProducts"], tableColumnsDictionary["EcomProducts"],
                        new string[] { "ProductVariantCounter" });

                    HandleIsKeyColumns(ecomProductsMapping, new string[] { "ProductVariantID", "ProductLanguageID" });
                }
            }
        }

        if (HasData("EcomManufacturers"))
        {
            List<Mapping>? manufacturersMappings = null;
            if (Mappings.TryGetValue("EcomManufacturers", out manufacturersMappings))
            {
                foreach (var mapping in manufacturersMappings)
                {
                    EnsureMapping(mapping, DestinationColumnMappings["EcomManufacturers"], tableColumnsDictionary["EcomManufacturers"],
                        new string[] { "ManufacturerID" });
                    EnsureMapping(mapping, DestinationColumnMappings["EcomManufacturers"], tableColumnsDictionary["EcomManufacturers"],
                        new string[] { "ManufacturerName" });
                }
            }
            else
            {
                Mapping ecomManufacturersMapping = job.AddMapping();
                EnsureMapping(ecomManufacturersMapping, null, tableColumnsDictionary["EcomManufacturers"], new string[] { "ManufacturerID" });
                EnsureMapping(ecomManufacturersMapping, null, tableColumnsDictionary["EcomManufacturers"], new string[] { "ManufacturerName" });
                job.Mappings.Add(ecomManufacturersMapping);
                _addedMappingsForMoveToMainTables.Add(ecomManufacturersMapping);
                ColumnMappingsByMappingId.Add(ecomManufacturersMapping.GetId(), ecomManufacturersMapping.GetColumnMappings(true));
            }
        }

        List<Mapping>? productCateforyMappings = null;
        if (Mappings.TryGetValue("EcomProductCategoryFieldValue", out productCateforyMappings))
        {
            foreach (var mapping in productCateforyMappings)
            {
                EnsureMapping(mapping, DestinationColumnMappings["EcomProductCategoryFieldValue"], tableColumnsDictionary["EcomProductCategoryFieldValue"],
                    new string[] { "FieldValueFieldCategoryId", "FieldValueProductId", "FieldValueProductLanguageId", "FieldValueProductVariantId" });
            }
        }

        if (HasData("EcomLanguages") && !Mappings.ContainsKey("EcomLanguages"))
        {
            Mapping ecomLanguagesMapping = job.AddMapping();

            EnsureMapping(ecomLanguagesMapping, null, tableColumnsDictionary["EcomLanguages"], new string[] { "LanguageID" });
            EnsureMapping(ecomLanguagesMapping, null, tableColumnsDictionary["EcomLanguages"], new string[] { "LanguageName", "LanguageNativeName" });
            job.Mappings.Remove(ecomLanguagesMapping);
            //Import languages should be first of all
            job.Mappings.Insert(0, ecomLanguagesMapping);
            _addedMappingsForMoveToMainTables.Add(ecomLanguagesMapping);
            ColumnMappingsByMappingId.Add(ecomLanguagesMapping.GetId(), ecomLanguagesMapping.GetColumnMappings(true));
        }

        List<Mapping>? priceMappings = null;
        if (HasData("EcomPrices") && Mappings.TryGetValue("EcomPrices", out priceMappings))
        {
            foreach (Mapping mapping in priceMappings)
            {
                EnsureMapping(mapping, DestinationColumnMappings["EcomPrices"], tableColumnsDictionary["EcomPrices"],
                    new string[] { "PriceID", "PriceUserId", "PriceUserGroupId" });
                Column currencyColumn = DestinationColumnMappings["EcomPrices"]["PriceCurrency"];
                if (currencyColumn != null)
                {
                    currencyColumn.IsPrimaryKey = false; //change identity to update currency values but not insert the new rows
                    if (!DestinationColumnMappings["EcomPrices"].ContainsKey("PriceCurrency"))
                    {
                        mapping.AddMapping(currencyColumn, currencyColumn, false);
                    }
                }
            }
        }

        List<Mapping>? discountMappings = null;
        if (HasData("EcomDiscount") && Mappings.TryGetValue("EcomDiscount", out discountMappings))
        {
            foreach (Mapping mapping in discountMappings)
            {
                EnsureMapping(mapping, DestinationColumnMappings["EcomDiscount"], tableColumnsDictionary["EcomDiscount"],
                    new string[] { "DiscountAccessUserId", "DiscountAccessUserGroupId" });
            }
        }

        List<Mapping>? detailsMappings = null;
        if (HasData("EcomDetails") && Mappings.TryGetValue("EcomDetails", out detailsMappings))
        {
            foreach (Mapping mapping in detailsMappings)
            {
                EnsureMapping(mapping, DestinationColumnMappings["EcomDetails"], tableColumnsDictionary["EcomDetails"], new string[] { "DetailID" });
            }
        }

        List<Mapping>? assortmentsMappings = null;
        if (HasData("EcomAssortmentPermissions") && Mappings.TryGetValue("EcomAssortmentPermissions", out assortmentsMappings))
        {
            foreach (Mapping mapping in assortmentsMappings)
            {
                EnsureMapping(mapping, DestinationColumnMappings["EcomAssortmentPermissions"], tableColumnsDictionary["EcomAssortmentPermissions"], new string[] { "AssortmentPermissionAccessUserID" });
            }
        }

        List<Mapping>? stockLocationMappings = null;
        if (Mappings.TryGetValue("EcomStockLocation", out stockLocationMappings))
        {
            foreach (var mapping in stockLocationMappings)
            {
                EnsureMapping(mapping, DestinationColumnMappings["EcomStockLocation"], tableColumnsDictionary["EcomStockLocation"],
                    new string[] { "StockLocationGroupId" });
            }
        }


        if (Mappings.TryGetValue("EcomUnits", out List<Mapping>? unitMappings))
        {
            foreach (var mapping in unitMappings)
            {
                EnsureMapping(mapping, DestinationColumnMappings["EcomUnits"], tableColumnsDictionary["EcomUnits"],
                    new string[] { "UnitId", "UnitExternalId" });
            }
        }


        if (Mappings.TryGetValue("EcomUnitTranslations", out List<Mapping>? unitTranslationMappings))
        {
            foreach (var mapping in unitTranslationMappings)
            {
                EnsureMapping(mapping, DestinationColumnMappings["EcomUnitTranslations"], tableColumnsDictionary["EcomUnitTranslations"],
                    new string[] { "UnitTranslationUnitId", "UnitTranslationLanguageId", "UnitTranslationName" });
            }
        }
    }

    private static void EnsureMapping(Mapping mapping, Dictionary<string, Column>? destinationColumns, Dictionary<string, Column> schemaColumns, string[] keyColumnNames)
    {
        foreach (var keyColumn in keyColumnNames)
        {
            if (destinationColumns == null || !destinationColumns.ContainsKey(keyColumn))
            {
                var groupKeyColumn = schemaColumns[keyColumn];
                mapping.AddMapping(groupKeyColumn, groupKeyColumn, false);
            }
        }
    }

    private void HandleIsKeyColumns(Mapping mapping, string[] destinationKeyColumnNames)
    {
        var columnMappings = mapping.GetColumnMappings();
        IEnumerable<ColumnMapping> keyColumnMappings = columnMappings.GetKeyColumnMappings();
        if (keyColumnMappings.Any())
        {
            foreach (var destinationKeyColumnName in destinationKeyColumnNames)
            {
                // If any Key columns are in the mappings we need to add ProductLanguageID as Key Column mapping as well
                if (!keyColumnMappings.Any((ColumnMapping cm) => string.Equals(cm.DestinationColumn?.Name, destinationKeyColumnName, StringComparison.OrdinalIgnoreCase)))
                {
                    var cm = columnMappings.Where(cm => cm != null && cm.Active &&
                            cm.DestinationColumn != null && string.Equals(cm.DestinationColumn?.Name, destinationKeyColumnName, StringComparison.OrdinalIgnoreCase)).FirstOrDefault();
                    if (cm is not null)
                    {
                        cm.IsKey = true;
                    }
                }
            }
        }
    }

    private void RemoveColumnMappingsFromJobThatShouldBeSkippedInMoveToMainTables()
    {
        List<Mapping>? mappings = null;

        if (Mappings.TryGetValue("EcomProducts", out mappings))
        {
            foreach (Mapping cleanMapping in mappings)
            {
                if (cleanMapping != null)
                {
                    ColumnMappingCollection columnMapping = cleanMapping.GetColumnMappings(true);
                    columnMapping.RemoveAll(cm => cm.DestinationColumn != null &&
                        (new HashSet<string>() { "RelatedProducts", "Groups", "PrimaryGroup", "GroupSorting", "VariantGroups", "VariantOptions" }.Contains(cm.DestinationColumn.Name, StringComparer.OrdinalIgnoreCase) || cm.DestinationColumn.Name.StartsWith("ProductCategory|", StringComparison.OrdinalIgnoreCase)));
                }
            }
        }

        if (Mappings.TryGetValue("EcomGroups", out mappings))
        {
            foreach (Mapping cleanMapping in mappings)
            {
                if (cleanMapping != null)
                {
                    ColumnMappingCollection columnMapping = cleanMapping.GetColumnMappings(true);
                    columnMapping.RemoveAll(cm => cm.DestinationColumn != null &&
                        new HashSet<string>() { "Shops", "ShopSorting", "ParentGroups", "ParentGroupsSorting", "VariantOptions" }.Contains(cm.DestinationColumn.Name, StringComparer.OrdinalIgnoreCase));
                }
            }
        }

        if (Mappings.TryGetValue("EcomProductsRelated", out mappings))
        {
            foreach (Mapping cleanMapping in mappings)
            {
                if (cleanMapping != null)
                {
                    ColumnMappingCollection columnMapping = cleanMapping.GetColumnMappings(true);
                    columnMapping.RemoveAll(cm => cm.DestinationColumn != null && string.Compare(cm.DestinationColumn.Name, "ProductRelatedLanguageID", true) == 0);
                }
            }
        }

        if (Mappings.TryGetValue("EcomProductCategoryFieldValue", out mappings))
        {
            foreach (Mapping cleanMapping in mappings)
            {
                if (cleanMapping != null)
                {
                    ColumnMappingCollection columnMapping = cleanMapping.GetColumnMappings(true);
                    columnMapping.RemoveAll(cm => cm.DestinationColumn != null && cm.DestinationColumn.Name == "FieldValueProductNumber");
                }
            }
        }

        if (Mappings.TryGetValue("EcomAssortmentPermissions", out mappings))
        {
            foreach (Mapping cleanMapping in mappings)
            {
                if (cleanMapping != null)
                {
                    ColumnMappingCollection columnMapping = cleanMapping.GetColumnMappings(true);
                    columnMapping.RemoveAll(cm => cm.DestinationColumn != null &&
                        new HashSet<string>() { "AssortmentPermissionCustomerNumber", "AssortmentPermissionExternalID" }.Contains(cm.DestinationColumn.Name, StringComparer.OrdinalIgnoreCase));
                }
            }
        }

        if (Mappings.TryGetValue("EcomDiscount", out mappings))
        {
            foreach (Mapping cleanMapping in mappings)
            {
                if (cleanMapping != null)
                {
                    ColumnMappingCollection columnMapping = cleanMapping.GetColumnMappings(true);
                    columnMapping.RemoveAll(cm => cm.DestinationColumn != null &&
                        new HashSet<string>() { "DiscountAccessUser", "DiscountAccessUserGroup" }.Contains(cm.DestinationColumn.Name, StringComparer.OrdinalIgnoreCase));
                }
            }
        }

        if (Mappings.TryGetValue("EcomPrices", out mappings))
        {
            foreach (Mapping cleanMapping in mappings)
            {
                if (cleanMapping != null)
                {
                    ColumnMappingCollection columnMapping = cleanMapping.GetColumnMappings(true);
                    columnMapping.RemoveAll(cm => cm.DestinationColumn != null &&
                        new HashSet<string>() { "PriceAccessUser", "PriceAccessUserGroup" }.Contains(cm.DestinationColumn.Name, StringComparer.OrdinalIgnoreCase));
                }
            }
        }
    }

    public void UpdateVariantFieldsInProducts()
    {
        foreach (DataTable productsDataTable in FindDataTablesStartingWithName("EcomProducts"))
        {
            var ecomVariantOptionsProductRelationTables = FindDataTablesStartingWithName("EcomVariantOptionsProductRelation");
            if (DataToWrite.Tables.Contains("EcomVariantgroupProductrelation") && ecomVariantOptionsProductRelationTables.Count() > 0)
            {
                bool variantCounterColExist = productsDataTable.Columns.Contains("ProductVariantCounter");

                if (!variantCounterColExist)
                {
                    continue;
                }

                Dictionary<string, int> productVariantCounterDict = new Dictionary<string, int>();
                Dictionary<string, List<DataRow>>? tableRows = null;
                if (DataRowsToWrite.TryGetValue(productsDataTable.TableName, out tableRows))
                {
                    foreach (DataRow row in tableRows.Values.SelectMany(c => c))
                    {
                        string productId = row["ProductID"].ToString() ?? "";
                        string langId = row["ProductLanguageID"].ToString() ?? "";
                        //Check if it is already existing product row and it has filled variants counter fileds - skip it
                        if ((row["ProductVariantCounter"] == System.DBNull.Value || row["ProductVariantCounter"].ToString() == "0") && variantCounterColExist)
                        {
                            int variantOptionsCounter = 0;
                            ProductVariantsCountDictionary.TryGetValue(string.Format("{0}.{1}", productId, langId), out variantOptionsCounter);
                            row["ProductVariantCounter"] = variantOptionsCounter > 0 ? variantOptionsCounter - 1 : 0;
                        }
                    }
                }
                productVariantCounterDict.Clear();
            }
        }
    }

    public void UpdateFieldsInExistingProductsWithVariantIDs()
    {
        if (Mappings.ContainsKey("EcomProducts"))
        {
            foreach (var mapping in Mappings["EcomProducts"])
            {
                if (!DestinationColumnMappings["EcomProducts"].ContainsKey("ProductVariantID"))
                {
                    string? keyColumn = "ProductNumber";

                    if (!DestinationColumnMappings["EcomProducts"].ContainsKey("ProductNumber"))
                    {
                        string[] columnsToSkip = new string[] { "ProductID", "ProductVariantID", "ProductLanguageID",
                            "ProductNumber", "ProductName" };

                        var ecomProductsPKColumns = MappingIdEcomProductsPKColumns.Keys.Count > 0 ?
                            MappingIdEcomProductsPKColumns[MappingIdEcomProductsPKColumns.Keys.First()] : null;
                        IEnumerable<string>? columnsToSearchForProductsToUpdate = ecomProductsPKColumns?.Where(c =>
                            !columnsToSkip.Any(cs => string.Equals(c, cs, StringComparison.OrdinalIgnoreCase)));

                        keyColumn = columnsToSearchForProductsToUpdate?.FirstOrDefault();
                        if (string.IsNullOrEmpty(keyColumn))
                        {
                            keyColumn = "ProductNumber";
                        }
                    }
                    UpdateFieldsInExistingProductsWithVariantIDs(keyColumn, mapping);
                }
            }
        }
    }

    public void UpdateFieldsInExistingProductsWithVariantIDs(string keyColumn, Mapping mapping)
    {
        Hashtable existigProductVariantIdsCombination = GetExistingProductVariantsIDsCombinations(keyColumn);
        if (existigProductVariantIdsCombination.Keys.Count > 0)
        {
            string? langId;
            string key;
            List<DataRow> rowsToAdd = new List<DataRow>();
            DataRow? newRow;
            var tableName = GetTableName("EcomProducts", mapping);

            var columns = DataToWrite?.Tables?[tableName]?.Columns;
            if (columns is not null && columns.Contains(keyColumn) && columns.Contains("ProductVariantID"))
            {
                foreach (var rows in DataRowsToWrite[tableName].Values)
                {
                    foreach (var row in rows)
                    {
                        if (!string.IsNullOrEmpty(row[keyColumn].ToString()))
                        {
                            langId = row["ProductLanguageID"].ToString();
                            if (!string.IsNullOrEmpty(langId))
                            {
                                key = string.Format("{0}.{1}", row[keyColumn].ToString(), langId);
                            }
                            else
                            {
                                key = row[keyColumn].ToString() ?? "";
                            }
                            if (existigProductVariantIdsCombination.ContainsKey(key))
                            {
                                string? rowProductVariantId = row["ProductVariantID"].ToString();
                                List<Tuple<string, string>>? variantsInfoList = (List<Tuple<string, string>>?)existigProductVariantIdsCombination[key];
                                if (variantsInfoList is not null)
                                {
                                    foreach (Tuple<string, string> variantInfo in variantsInfoList)
                                    {
                                        if (string.IsNullOrEmpty(rowProductVariantId) || !string.Equals(rowProductVariantId, variantInfo.Item1))
                                        {
                                            newRow = DataToWrite?.Tables?[tableName]?.NewRow();
                                            if (newRow is not null)
                                            {
                                                if (row?.ItemArray is not null)
                                                {
                                                    newRow.ItemArray = (object?[])row.ItemArray.Clone();
                                                }
                                                newRow["ProductVariantID"] = variantInfo.Item1;
                                                newRow["ProductVariantCounter"] = variantInfo.Item2;
                                                rowsToAdd.Add(newRow);
                                            }
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }

            foreach (DataRow dt in rowsToAdd)
            {
                DataRowsToWrite[tableName].Add(RowAutoId++.ToString(), new List<DataRow>() { dt });
            }
        }
    }

    private Hashtable GetExistingProductVariantsIDsCombinations(string searchingDifferentProductsColumn)
    {
        Hashtable result = new Hashtable();
        DataRow[] rows = ExistingProducts.Select("ProductVariantID <> ''");
        string key;
        string? languageId = null;

        foreach (DataRow row in rows)
        {
            if (!string.IsNullOrEmpty(row[searchingDifferentProductsColumn].ToString()))
            {
                languageId = row["ProductLanguageID"].ToString();
                if (!string.IsNullOrEmpty(languageId))
                {
                    key = string.Format("{0}.{1}", row[searchingDifferentProductsColumn].ToString(), languageId);
                }
                else
                {
                    key = row[searchingDifferentProductsColumn].ToString() ?? "";
                }

                Tuple<string?, string?> variantInfo = new Tuple<string?, string?>
                    (
                        row["ProductVariantID"].ToString(),
                        row["ProductVariantCounter"].ToString()
                    );
                if (result.ContainsKey(key))
                {
                    var value = result[key];
                    if (value is not null)
                    {
                        List<Tuple<string?, string?>> variantIDsList = (List<Tuple<string?, string?>>)value;
                        variantIDsList?.Add(variantInfo);
                    }
                }
                else
                {
                    result[key] = new List<Tuple<string?, string?>>() { variantInfo };
                }
            }
        }
        return result;
    }

    public void UpdateProductRelatedProducts()
    {
        foreach (DataTable dataTable in FindDataTablesStartingWithName("EcomProducts"))
        {
            if (dataTable.Columns.Contains("RelatedProducts") &&
                dataTable.Columns.Contains("ProductID") &&
                dataTable.Columns.Contains("ProductLanguageID"))
            {
                var tables = FindDataTablesStartingWithName("EcomProductsRelated");
                if (tables.Count() > 0)
                {
                    //Note the related products will be updated just in the first found EcomProductsRelated table
                    //this is because if we have several EcomProductsRelated tables in the mapping we can not distinguish what table to use
                    //so the best choice is to have just one EcomProductsRelated table in the mapping

                    DataTable ecomProductsRelatedDataTable = tables.First();
                    bool productsRelatedTableIsPresent = false; //ecomProductsRelatedDataTable.Rows.Count > 0;
                    Dictionary<string, List<DataRow>>? ecomProductsRelatedDataTableRows = null;
                    if (DataRowsToWrite.TryGetValue(ecomProductsRelatedDataTable.TableName, out ecomProductsRelatedDataTableRows))
                    {
                        productsRelatedTableIsPresent = ecomProductsRelatedDataTableRows.Values.SelectMany(c => c).Any();
                    }
                    Dictionary<string, List<DataRow>>? tableRows = null;
                    if (DataRowsToWrite.TryGetValue(dataTable.TableName, out tableRows))
                    {
                        foreach (DataRow row in tableRows.Values.SelectMany(c => c))
                        {
                            var relatedProductIdsStr = row["RelatedProducts"].ToString();
                            var relatedProductIds = SplitOnComma(relatedProductIdsStr);
                            for (int i = 0; i < relatedProductIds.Length; i++)
                            {
                                string? relatedGroupID = string.Empty;
                                if (productsRelatedTableIsPresent)
                                {
                                    var filter = new Func<DataRow, bool>(r => r["ProductRelatedProductID"].ToString() == row["ProductID"].ToString() && r["ProductRelatedProductRelID"].ToString() == relatedProductIds[i]);
                                    DataRow? productsRelRow = FindRow(ecomProductsRelatedDataTable.TableName, filter);
                                    //DataRow[] productsRelatedRows = ecomProductsRelatedDataTable.Select("ProductRelatedProductID='" + row["ProductID"].ToString().Replace("'", "''") + "' and ProductRelatedProductRelID='" + relatedProductIds[i].Replace("'", "''") + "'");
                                    if (productsRelRow != null)
                                    {
                                        relatedGroupID = productsRelRow["ProductRelatedGroupID"].ToString();
                                    }
                                }
                                if (string.IsNullOrEmpty(relatedGroupID))
                                {
                                    relatedGroupID = GetDefaultGroupID(row["ProductLanguageID"].ToString() ?? "");
                                }

                                AddRelatedProductReferenceToProduct(dataTable, tableRows, ecomProductsRelatedDataTable, row["ProductID"].ToString(), relatedProductIds[i], relatedGroupID);
                            }
                        }
                    }
                }
                else
                {
                    throw new Exception("Can not find any EcomProductsRelated table");
                }
            }
        }
    }

    private void AddRelatedProductReferenceToProduct(DataTable ecomProductsDataTable, Dictionary<string, List<DataRow>> ecomProductsDataTableRows, DataTable ecomProductsRelatedDataTable, string? productID, string relatedProduct, string? relatedGroupID)
    {
        var filter = new Func<DataRow, bool>(r => r["ProductID"].ToString() == relatedProduct);
        //find ProductID by relatedProduct string(it can contain ID, Number, Name)
        var rows = FindExistingRows(ecomProductsDataTableRows, relatedProduct, filter);
        if (rows?.Count == 0 && !useStrictPrimaryKeyMatching)
        {
            if (ecomProductsDataTable.Columns.Contains("ProductNumber"))
            {
                filter = new Func<DataRow, bool>(r => r["ProductNumber"].ToString() == relatedProduct);
                rows = FindExistingRows(ecomProductsDataTableRows, string.Empty, filter);
            }
            if (rows?.Count == 0)
            {
                if (ecomProductsDataTable.Columns.Contains("ProductName"))
                {
                    filter = new Func<DataRow, bool>(r => r["ProductName"].ToString() == relatedProduct);
                    rows = FindExistingRows(ecomProductsDataTableRows, string.Empty, filter);
                }
                if (rows?.Count == 0)
                {
                    DataRow? row = GetExistingProductDataRow(relatedProduct);
                    if (row != null)
                    {
                        rows = new List<DataRow>() { row };
                    }
                }
            }
        }
        if (rows?.Count > 0)//if Product found
        {
            string? relatedProductID = rows[0]["ProductID"].ToString();

            filter = new Func<DataRow, bool>(r => r["ProductRelatedProductID"].ToString() == productID && r["ProductRelatedProductRelID"].ToString() == relatedProductID && r["ProductRelatedGroupID"].ToString() == relatedGroupID);
            //string filter = string.Format("ProductRelatedProductID='{0}' and ProductRelatedProductRelID='{1}' and ProductRelatedGroupID='{2}'", productID, relatedProductID, relatedGroupID);
            var row = FindRow(ecomProductsRelatedDataTable.TableName, filter);
            if (row == null)
            {
                var productRelation = ecomProductsRelatedDataTable.NewRow();
                productRelation["ProductRelatedProductID"] = productID;
                productRelation["ProductRelatedProductRelID"] = relatedProductID;
                productRelation["ProductRelatedGroupID"] = relatedGroupID;

                Dictionary<string, List<DataRow>>? productRelations = null;
                if (!DataRowsToWrite.TryGetValue(ecomProductsRelatedDataTable.TableName, out productRelations))
                {
                    productRelations = new Dictionary<string, List<DataRow>>();
                    DataRowsToWrite.Add(ecomProductsRelatedDataTable.TableName, productRelations);
                }
                productRelations.Add(RowAutoId++.ToString(), new List<DataRow>() { productRelation });
            }
        }
    }

    //Returns existing ProductID, if Product is not found returns id like "ImportedPROD"LastProductId
    private DataRow? GetExistingProduct(Dictionary<string, object> row, Mapping mapping, ColumnMapping? productNumberColumn, ColumnMapping? productNameColumn)
    {
        DataRow? result = GetExistingProductByPKColumns(row, mapping);

        if (result == null && !useStrictPrimaryKeyMatching)
        {
            MappingIdEcomProductsPKColumns.TryGetValue(mapping.GetId(), out var ecomProductsPKColumns);
            if (ecomProductsPKColumns == null || !ecomProductsPKColumns.Contains("ProductNumber"))
            {
                //search existing products by ProductNumber
                if (productNumberColumn != null)
                {
                    string? productNumber = GetValue(productNumberColumn, row);
                    if (!string.IsNullOrEmpty(productNumber))
                    {
                        var rows = ExistingProducts.Select("ProductNumber='" + productNumber.Replace("'", "''") + "'");
                        if (rows.Length > 0)
                        {
                            result = rows[0];
                        }
                    }
                }
            }
            if (result == null && (ecomProductsPKColumns == null || !ecomProductsPKColumns.Contains("ProductName")))
            {
                //search existing products by ProductName
                if (productNameColumn != null)
                {
                    string? productName = GetValue(productNameColumn, row);
                    if (!string.IsNullOrEmpty(productName))
                    {
                        var rows = ExistingProducts.Select("ProductName='" + productName.Replace("'", "''") + "'");
                        if (rows.Length > 0)
                        {
                            result = rows[0];
                        }
                    }
                }
            }
        }

        return result;
    }

    private Dictionary<int, IEnumerable<string>> GetEcomProductsPKColumns()
    {
        Dictionary<int, IEnumerable<string>> result = new Dictionary<int, IEnumerable<string>>();
        if (Mappings.TryGetValue("EcomProducts", out List<Mapping>? productsMappings))
        {
            foreach (var mapping in productsMappings)
            {
                if (!result.ContainsKey(mapping.GetId()))
                {
                    var columnMappings = mapping.GetColumnMappings();
                    result.Add(mapping.GetId(),
                        columnMappings.Where(cm => cm != null && cm.Active &&
                        cm.DestinationColumn != null && !string.IsNullOrEmpty(cm.DestinationColumn.Name) &&
                        cm.DestinationColumn.IsKeyColumn(columnMappings)).Select(cm => cm.DestinationColumn.Name));
                }
            }
        }
        return result;
    }

    private DataRow? GetExistingProductByPKColumns(Dictionary<string, object> row, Mapping mapping)
    {
        DataRow? result = null;

        MappingIdEcomProductsPKColumns.TryGetValue(mapping.GetId(), out var ecomProductsPKColumns);
        if (ecomProductsPKColumns?.Count() > 0)
        {
            string query = string.Empty;
            foreach (string column in ecomProductsPKColumns)
            {
                var columnMapping = mapping.GetColumnMappings().Find(cm => cm != null && cm.Active &&
                    string.Equals(cm.DestinationColumn?.Name, column, StringComparison.OrdinalIgnoreCase));

                if (columnMapping != null && !string.IsNullOrEmpty(columnMapping.SourceColumn?.Name))
                {
                    string? value = GetValue(columnMapping, row);
                    if (!string.IsNullOrEmpty(value))
                    {
                        query += $"{column}='" + value.Replace("'", "''") + "' AND ";
                    }
                }
            }

            if (query.EndsWith(" AND "))
            {
                var rows = ExistingProducts.Select(query.Substring(0, query.Length - " AND ".Length));
                if (rows.Length > 0)
                {
                    result = rows[0];
                }
            }
        }

        return result;
    }

    /// <summary>
    /// Search for existing product by ProductID, if not found then searches by ProductNumber, and then by ProductName
    /// </summary>
    /// <param name="searchString"></param>
    /// <returns>Returns existing DataRow with product information if found, otherwise null</returns>
    private DataRow? GetExistingProductDataRow(string searchString)
    {
        DataRow? result = null;
        DataRow[] rows = ExistingProducts.Select("ProductID='" + searchString + "'");
        if (rows.Length > 0)
        {
            result = rows[0];
        }
        else
        {
            rows = ExistingProducts.Select("ProductNumber='" + searchString + "'");
            if (rows.Length > 0)
            {
                result = rows[0];
            }
            else
            {
                rows = ExistingProducts.Select("ProductName='" + searchString + "'");
                if (rows.Length > 0)
                {
                    result = rows[0];
                }
            }
        }
        return result;
    }

    private StockLocation? GetExistingStockLocation(Dictionary<string, object> row, ColumnMapping stockLocationIdColumn)
    {
        StockLocation? result = null;
        if (stockLocationIdColumn != null)
        {
            var stockLocationId = GetValue(stockLocationIdColumn, row);
            if (!string.IsNullOrEmpty(stockLocationId))
            {
                if (long.TryParse(stockLocationId, out var stockLocationIdAsLong))
                {
                    result = Ecommerce.Services.StockService.GetStockLocation(stockLocationIdAsLong);
                }

                if (result == null)
                {
                    var defaultLanguageId = Ecommerce.Services.Languages.GetDefaultLanguageId();
                    foreach (var location in Ecommerce.Services.StockService.GetStockLocations())
                    {
                        if (location.GetName(defaultLanguageId).Equals(stockLocationId, StringComparison.OrdinalIgnoreCase))
                        {
                            return location;
                        }
                    }
                }
            }
        }
        return result;
    }

    //Returns existing ManufacturerID if found Manufacturer by ManufacturerName. Returns null if no manufacturer found.
    private DataRow? GetExistingManufacturer(Dictionary<string, object> row, ColumnMapping? manufacturerNameColumn)
    {
        DataRow? result = null;
        if (manufacturerNameColumn != null)
        {
            string? manufacturerName = GetValue(manufacturerNameColumn, row);
            if (!string.IsNullOrEmpty(manufacturerName))
            {
                foreach (var manufactorerRow in ProductManufacturers.Values)
                {
                    var existingManufacturerName = Converter.ToString(manufactorerRow["ManufacturerName"]);
                    if (string.Equals(existingManufacturerName, manufacturerName))
                    {
                        return manufactorerRow;
                    }
                }
            }
        }
        return result;
    }

    internal new void Close()
    {
        foreach (DataTable table in DataToWrite.Tables)
        {
            string tableName = GetTableNameWithoutPrefix(table.TableName) + TempTableName + GetPrefixFromTableName(table.TableName);
            sqlCommand.CommandText = $"if exists (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'{tableName}') AND type in (N'U')) drop table [{tableName}]";
            sqlCommand.ExecuteNonQuery();
        }

        ClearHashTables();
        if (duplicateRowsHandler != null)
        {
            duplicateRowsHandler.Dispose();
        }
        _rowsWithMissingGroups = new List<Dictionary<string, object>>();
    }

    private void ClearHashTables()
    {
        ecomVariantOptionsProductRelationKeys.Clear();
        ecomGroupProductRelationKeys.Clear();
        ecomVariantgroupProductrelationKeys.Clear();
    }

    internal void CleanRelationsTables(SqlTransaction transaction)
    {
        sqlCommand.Transaction = transaction;
        sqlCommand.CommandText = "delete from EcomGroupProductRelation where not exists(select * from EcomProducts where ProductID=GroupProductRelationProductID) or not exists(select * from ecomGroups where GroupID=GroupProductRelationGroupID);";
        sqlCommand.ExecuteNonQuery();
        sqlCommand.Transaction = transaction;
        sqlCommand.CommandText = "delete from EcomShopGroupRelation where not exists (select * from EcomGroups where GroupID=ShopGroupGroupID)";
        sqlCommand.ExecuteNonQuery();
        sqlCommand.Transaction = transaction;
        sqlCommand.CommandText = "DELETE FROM [EcomAssortmentProductRelations] WHERE ( NOT EXISTS( SELECT [EcomProducts].[ProductAutoID] FROM [EcomProducts] WHERE ( [EcomProducts].[ProductID] = [EcomAssortmentProductRelations].[AssortmentProductRelationProductID] ) AND ( [EcomProducts].[ProductVariantID] = [EcomAssortmentProductRelations].[AssortmentProductRelationProductVariantID] ) ) )";
        sqlCommand.ExecuteNonQuery();
        sqlCommand.Transaction = transaction;
        sqlCommand.CommandText = "DELETE FROM [EcomAssortmentItems] WHERE ( [EcomAssortmentItems].[AssortmentItemRelationType] = 'PRODUCT' ) AND ( NOT EXISTS( SELECT * FROM [EcomAssortmentProductRelations] WHERE ( [EcomAssortmentProductRelations].[AssortmentProductRelationAutoID] = [EcomAssortmentItems].[AssortmentItemRelationAutoID] ) ) )";
        sqlCommand.ExecuteNonQuery();
        sqlCommand.Transaction = transaction;
        sqlCommand.CommandText = "DELETE FROM [EcomAssortmentGroupRelations] WHERE ( NOT EXISTS( SELECT [EcomGroups].[GroupID] FROM [EcomGroups] WHERE ( [EcomGroups].[GroupID] = [EcomAssortmentGroupRelations].[AssortmentGroupRelationGroupID] ) ) )";
        sqlCommand.ExecuteNonQuery();
        sqlCommand.Transaction = transaction;
        sqlCommand.CommandText = "DELETE FROM [EcomAssortmentItems] WHERE ( [EcomAssortmentItems].[AssortmentItemRelationType] = 'GROUP' ) AND ( NOT EXISTS( SELECT * FROM [EcomAssortmentGroupRelations] WHERE ( [EcomAssortmentGroupRelations].[AssortmentGroupRelationAutoID] = [EcomAssortmentItems].[AssortmentItemRelationAutoID] ) ) )";
        sqlCommand.ExecuteNonQuery();
    }

    internal void RebuildAssortments()
    {
        assortmentHandler?.RebuildAssortments();
    }

    private void RemoveExcessFromRelationsTables(SqlTransaction sqlTransaction)
    {
        sqlCommand.Transaction = sqlTransaction;
        sqlCommand.CommandText =
            "delete t1 from EcomGroupProductRelation t1 INNER JOIN ecomgroupproductrelationTempTableForBulkImport t2 ON t1.GroupProductRelationGroupID = t2.GroupProductRelationGroupID AND t1.GroupProductRelationProductID = t2.GroupProductRelationProductID;" +
            "insert into EcomGroupProductRelation (GroupProductRelationGroupID,GroupProductRelationProductID,GroupProductRelationSorting,GroupProductRelationIsPrimary) select GroupProductRelationGroupId, GroupProductRelationProductID,GroupProductRelationSorting,GroupProductRelationIsPrimary from ecomgroupproductrelationTempTableForBulkImport; " +
            "delete from EcomShopGroupRelation where ShopGroupGroupID in (select ShopGroupGroupID from EcomShopGroupRelationTempTableForBulkImport); " +
            "insert into EcomShopGroupRelation(ShopGroupShopID,ShopGroupGroupID,ShopGroupRelationsSorting) select shopgroupshopid,shopgroupgroupid,ShopGroupRelationsSorting from ecomshopgrouprelationtemptableforbulkimport; " +
            "delete from EcomVariantgroupProductRelation where VariantgroupProductRelationProductID in (select VariantgroupProductRelationProductID from EcomVariantgroupProductRelationTempTableForBulkImport); " +
            "insert into EcomVariantgroupProductRelation (VariantgroupProductRelationID,VariantgroupProductRelationProductID,VariantgroupProductRelationVariantGroupID,VariantGroupProductRelationSorting)select VariantgroupProductRelationID,VariantgroupProductRelationProductID,VariantgroupProductRelationVariantGroupID,VariantGroupProductRelationSorting from EcomVariantgroupProductRelationTempTableForBulkImport;";

        foreach (DataTable table in FindDataTablesStartingWithName("EcomVariantOptionsProductRelation"))
        {
            string tempTableName = GetTableNameWithoutPrefix(table.TableName) + TempTableName + GetPrefixFromTableName(table.TableName);
            if (deleteExcess || removeMissingAfterImportDestinationTablesOnly)
            {
                sqlCommand.CommandText += $"delete from EcomVariantOptionsProductRelation where VariantOptionsProductRelationProductID in (select VariantOptionsProductRelationProductID from {tempTableName}); ";
            }
            else
            {
                sqlCommand.CommandText += $"delete t1 from EcomVariantOptionsProductRelation t1 INNER JOIN {tempTableName} t2 ON t1.VariantOptionsProductRelationProductId = t2.VariantOptionsProductRelationProductId AND t1.VariantOptionsProductRelationVariantId = t2.VariantOptionsProductRelationVariantId;";
            }

            sqlCommand.CommandText += $"insert into EcomVariantOptionsProductRelation (VariantOptionsProductRelationProductID,VariantOptionsProductRelationVariantID)select VariantOptionsProductRelationProductID,VariantOptionsProductRelationVariantID from {tempTableName};";
        }

        foreach (DataTable table in FindDataTablesStartingWithName("EcomShops"))
        {
            string tempTableName = GetTableNameWithoutPrefix(table.TableName) + TempTableName + GetPrefixFromTableName(table.TableName);
            sqlCommand.CommandText += $"insert into EcomShops (ShopID,ShopName) select shopid,shopname from {tempTableName}; ";
        }

        sqlCommand.CommandText += "insert into EcomProductsRelatedGroups (RelatedGroupID,RelatedGroupName,RelatedGroupLanguageID) select RelatedGroupID,RelatedGroupName,RelatedGroupLanguageID from EcomProductsRelatedGroupsTempTableForBulkImport; ";

        foreach (DataTable table in FindDataTablesStartingWithName("EcomProductsRelated"))
        {
            string tempTableName = GetTableNameWithoutPrefix(table.TableName) + TempTableName + GetPrefixFromTableName(table.TableName);
            sqlCommand.CommandText += "delete from related from EcomProductsRelated related where ProductRelatedProductID in " +
                $"(select ProductRelatedProductID from {tempTableName} inside WHERE related.ProductRelatedProductID = inside.ProductRelatedProductID AND " +
                "related.ProductRelatedProductRelID = inside.ProductRelatedProductRelID AND related.ProductRelatedGroupID = inside.ProductRelatedGroupID AND related.ProductRelatedProductRelVariantID = inside.ProductRelatedProductRelVariantID); ";
            sqlCommand.CommandText += $"insert into EcomProductsRelated (ProductRelatedProductID,ProductRelatedProductRelID,ProductRelatedGroupID,ProductRelatedProductRelVariantID) select ProductRelatedProductID,ProductRelatedProductRelID,ProductRelatedGroupID,ProductRelatedProductRelVariantID from {tempTableName}; ";
        }

        sqlCommand.CommandText += "delete from EcomGroupRelations where groupRelationsGroupID in (select groupRelationsGroupID from EcomGroupRelationsTempTableForBulkImport);";
        sqlCommand.CommandText += "insert into EcomGroupRelations (GroupRelationsGroupID,GroupRelationsParentID,GroupRelationsSorting) select GroupRelationsGroupID,GroupRelationsParentID,GroupRelationsSorting from EcomGroupRelationsTempTableForBulkImport;";

        try
        {
            RowsAffected += sqlCommand.ExecuteNonQuery();
        }
        catch (Exception ex)
        {
            string msg = string.Format("Exception: {0} Sql query: {1}", ex.Message, sqlCommand.CommandText);
            if (ex.Message.Contains("The conflict occurred in database \"head\", table \"dbo.EcomShops\", column 'ShopID'."))
            {
                throw new Exception("Import attempted with  ShopID that does not exist in the database. Check shopID values in input data. " + msg, ex);
            }
            throw new Exception(msg, ex);
        }
    }

    private void DeleteExcessFromGroupProductRelation(string shop, SqlTransaction sqlTransaction)
    {
        sqlCommand.Transaction = sqlTransaction;
        try
        {
            StringBuilder? sqlClean = null;
            if (partialUpdate)
            {
                sqlClean = new StringBuilder();
                foreach (DataTable table in FindDataTablesStartingWithName("EcomProducts"))
                {
                    string tempTableName = GetTableNameWithoutPrefix(table.TableName) + TempTableName + GetPrefixFromTableName(table.TableName);
                    sqlClean.Append($"delete EcomGroupProductRelation from {tempTableName} join ecomgroupproductrelation on {tempTableName}.productid=ecomgroupproductrelation.GroupProductRelationProductID where not exists (select * from [dbo].[EcomGroupProductRelationTempTableForBulkImport] where [dbo].[EcomGroupProductRelation].[GroupProductRelationProductID]=[GroupProductRelationProductID] and [dbo].[EcomGroupProductRelation].[GroupProductRelationGroupID]=[GroupProductRelationGroupID] );");
                }
            }
            else
            {
                if (!string.IsNullOrEmpty(shop))
                {
                    sqlClean = new StringBuilder("DELETE [EcomGroupProductRelation] FROM [EcomGroupProductRelation] ");
                    sqlClean.Append("INNER JOIN [EcomShopGroupRelation] ON [GroupProductRelationGroupID] = [ShopGroupGroupID] WHERE NOT EXISTS(");
                    sqlClean.Append("SELECT GroupProductRelationGroupID, GroupProductRelationProductID FROM [EcomGroupProductRelationTempTableForBulkImport] WHERE ");
                    sqlClean.Append("[EcomGroupProductRelation].[GroupProductRelationGroupID] = [EcomGroupProductRelationTempTableForBulkImport].[GroupProductRelationGroupID] and [EcomGroupProductRelation].[GroupProductRelationProductID] = [EcomGroupProductRelationTempTableForBulkImport].[GroupProductRelationProductID]) ");
                    sqlClean.Append(string.Format("AND [EcomShopGroupRelation].[ShopGroupShopID] = '{0}'", shop));
                }
                else
                {
                    sqlClean = new StringBuilder("DELETE FROM [EcomGroupProductRelation] WHERE NOT EXISTS(");
                    sqlClean.Append("SELECT GroupProductRelationGroupID, GroupProductRelationProductID FROM [EcomGroupProductRelationTempTableForBulkImport] WHERE ");
                    sqlClean.Append("[EcomGroupProductRelation].[GroupProductRelationGroupID] = [EcomGroupProductRelationTempTableForBulkImport].[GroupProductRelationGroupID] and [EcomGroupProductRelation].[GroupProductRelationProductID] = [EcomGroupProductRelationTempTableForBulkImport].[GroupProductRelationProductID])");
                }
            }
            if (sqlClean != null && sqlClean.Length > 0)
            {
                sqlCommand.CommandText = sqlClean.ToString();
                RowsAffected += sqlCommand.ExecuteNonQuery();
            }
        }
        catch (Exception ex)
        {
            string msg = "Failed to delete product group relations from EcomGroupProductRelation relation table. ";
            msg += "Exception message: " + ex.Message + " Sql query: " + sqlCommand.CommandText;
            throw new Exception(msg, ex);
        }
    }

    private bool HasRowsToImport(Mapping? mapping, out string tempTablePrefix)
    {
        bool result = false;
        tempTablePrefix = TempTableName + mapping?.GetId();

        if (mapping != null && mapping.DestinationTable != null && mapping.DestinationTable.Name != null && DataToWrite != null && DataToWrite.Tables != null)
        {
            string destinationTableName = GetTableName(mapping.DestinationTable.Name, mapping);
            Dictionary<string, List<DataRow>>? rows = null;
            if (DataRowsToWrite.TryGetValue(destinationTableName, out rows) && rows.Values.Count > 0)
            {
                result = true;
            }
            else if (DataRowsToWrite.TryGetValue(mapping.DestinationTable.Name, out rows) && rows.Values.Count > 0)
            {
                tempTablePrefix = TempTableName;
                result = true;
            }
        }
        return result;
    }

    private DataRow? FindRow(string tableName, string id)
    {
        DataRow[] ret = new DataRow[0];

        Dictionary<string, List<DataRow>>? table = null;
        if (!DataRowsToWrite.TryGetValue(tableName, out table))
        {
            foreach (var key in DataRowsToWrite.Keys)
            {
                if (key.StartsWith(tableName + "$", StringComparison.InvariantCultureIgnoreCase))
                {
                    table = DataRowsToWrite[key];
                }
            }
        }
        return table != null && table.ContainsKey(id) ? table[id][0] : null;
    }

    private DataTable? FindDataTableByName(string tableName)
    {
        foreach (DataTable table in DataToWrite.Tables)
        {
            if (string.Compare(table.TableName, tableName, true) == 0)
            {
                return table;
            }
        }
        return null;
    }

    private IEnumerable<DataTable> FindDataTablesStartingWithName(string tableName)
    {
        List<DataTable> ret = new List<DataTable>();
        foreach (DataTable table in DataToWrite.Tables)
        {
            if (string.Compare(table.TableName, tableName, true) == 0 || table.TableName.StartsWith(tableName + "$", StringComparison.InvariantCultureIgnoreCase))
            {
                ret.Add(table);
            }
        }
        return ret;
    }

    private DataRow GetDataTableNewRow(string tableName)
    {
        DataRow? row = null;
        DataTable? table = FindDataTableByName(tableName);
        if (table == null)
        {
            foreach (DataTable foundTable in FindDataTablesStartingWithName(tableName))
            {
                row = foundTable.NewRow();
                break;
            }
            if (row == null)
            {
                //this should never happen
                throw new Exception($"Can not find table {tableName} in the DataToWrite.Tables");
            }
        }
        else
        {
            row = table.NewRow();
        }
        return row;
    }

    private bool HasData(string tableName)
    {
        foreach (DataTable table in DataToWrite.Tables)
        {
            if (table.TableName.StartsWith(tableName) && table.Rows.Count > 0)
            {
                return true;
            }
        }
        return false;
    }

    private string? GetValue(ColumnMapping? columnMapping, Dictionary<string, object> row)
    {
        string? result = null;        
        if (columnMapping != null && (columnMapping.HasScriptWithValue || row.ContainsKey(columnMapping.SourceColumn.Name)))
        {
            switch (columnMapping.ScriptType)
            {
                case ScriptType.None:
                    result = Converter.ToString(row[columnMapping.SourceColumn.Name]);
                    break;
                case ScriptType.Append:
                    result = Converter.ToString(row[columnMapping.SourceColumn.Name]) + Converter.ToString(columnMapping.ScriptTypeProvider?.GetValue(""));
                    break;
                case ScriptType.Prepend:
                    result = Converter.ToString(columnMapping.ScriptTypeProvider?.GetValue("")) + Converter.ToString(row[columnMapping.SourceColumn.Name]);
                    break;
                case ScriptType.Constant:
                    result = columnMapping.GetScriptValue();
                    break;
                case ScriptType.NewGuid:
                    result = columnMapping.GetScriptValue();
                    break;
            }
        }
        return result;
    }

    private string? GetMergedValue(ColumnMapping? columnMapping, Dictionary<string, object> row)
    {
        string? result = null;
        if (columnMapping == null) return result;
        if (columnMapping.DestinationColumn == null) return result;
        foreach (var item in columnMapping.Mapping.GetColumnMappings().Where(obj => obj.DestinationColumn.Name == columnMapping.DestinationColumn.Name))
        {
            object? rowValue = null;
            if (columnMapping.HasScriptWithValue || row.TryGetValue(item.SourceColumn?.Name ?? "", out rowValue))
            {
                object dataToRow = columnMapping.ConvertInputValueToOutputValue(rowValue);

                if (columnMapping.GetId() < item.GetId())
                {
                    result += Converter.ToString(dataToRow);
                }
                else
                {
                    result = Converter.ToString(dataToRow.ToString());
                }
            }
        }
        return result;
    }

    private void CountProductVariantGroups(string productID, string variantOptionID)
    {
        int productvariantGroupsCount = 0;
        if (ProductVariantGroupsCountDictionary.TryGetValue(productID, out productvariantGroupsCount))
        {
            int currentVariantGroupsCount = variantOptionID.Split(new char[] { '.' }).Length;
            if (currentVariantGroupsCount > productvariantGroupsCount)
            {
                ProductVariantGroupsCountDictionary[productID] = currentVariantGroupsCount;
            }
        }
        else
        {
            ProductVariantGroupsCountDictionary[productID] = 1;
        }
    }

    internal void UpdateGroupRelations()
    {
        bool isGroupIdInMapping = job.Mappings.Any(m => m?.DestinationTable?.Name == "EcomGroups" && m.GetColumnMappings().Any(cm => cm.Active && string.Equals(cm?.DestinationColumn.Name, "GroupId", StringComparison.OrdinalIgnoreCase)));
        if (!isGroupIdInMapping)
        {
            bool isParentGroupsInMapping = job.Mappings.Any(m => m?.DestinationTable.Name == "EcomGroups" && m.GetColumnMappings().Any(cm => cm.Active && string.Equals(cm?.DestinationColumn.Name, "ParentGroups", StringComparison.OrdinalIgnoreCase)));
            var rowsToWrite = DataRowsToWrite["EcomGroupRelations"];
            if (isParentGroupsInMapping && rowsToWrite != null && rowsToWrite.Count > 0)
            {
                Dictionary<string, string> searchResults = new Dictionary<string, string>();
                string? groupId, group;

                foreach (DataRow row in rowsToWrite.Values.SelectMany(c => c))
                {
                    group = row["GroupRelationsParentID"].ToString() ?? "";
                    if (!searchResults.TryGetValue(group, out groupId))
                    {
                        groupId = FindGroupId(group);
                        searchResults.Add(group, groupId);
                    }
                    if (!string.IsNullOrEmpty(groupId))
                    {
                        row["GroupRelationsParentID"] = groupId;
                    }
                }
            }
        }
    }

    private string FindGroupId(string value)
    {
        string groupId = string.Empty;
        foreach (string key in DataRowsToWrite.Keys)
        {
            if (key.StartsWith("EcomGroups"))
            {
                foreach (List<DataRow> dataRows in DataRowsToWrite[key].Values)
                {
                    foreach (var dataRow in dataRows)
                    {
                        var groupName = Converter.ToString(dataRow["GroupName"]);
                        if (string.Equals(groupName, value, StringComparison.OrdinalIgnoreCase))
                        {
                            return Converter.ToString(dataRow["GroupId"]);
                        }
                    }
                }
            }
        }
        if (string.IsNullOrEmpty(groupId))
        {
            foreach (var groupRows in ProductGroups.Values)
            {
                foreach (var groupRow in groupRows)
                {
                    var groupName = Converter.ToString(groupRow["GroupName"]);
                    if (groupName.Equals(value))
                    {
                        return Converter.ToString(groupRow["GroupId"]);
                    }
                }
            }
        }
        return groupId;
    }

    private string GetImportedProductsByNumberMultipleProductsIdentifier(string productNumber, string languageId)
    {
        return $"{productNumber}.{languageId}";
    }

    internal void FailOnMissingGroups()
    {
        if (_rowsWithMissingGroups?.Count > 0)
        {
            throw new Exception(EcomProductsMissingGroupsErrorMessage);
        }
    }

    internal void LogFailedRows()
    {
        logger.Log(EcomProductsMissingGroupsErrorMessage + ":");
        if (_rowsWithMissingGroups?.Count > 0)
        {
            foreach (var row in _rowsWithMissingGroups)
            {
                string rowData = "Failed row:";
                foreach (var kvp in row)
                {
                    rowData += $"\t [{kvp.Key}: \"{kvp.Value}\"],";
                }
                rowData = rowData.TrimStart().TrimEnd(',');
                logger.Log(rowData);
            }
        }
    }
}
