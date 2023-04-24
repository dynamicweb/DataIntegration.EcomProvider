using Dynamicweb.Core;
using Dynamicweb.DataIntegration.Integration;
using Dynamicweb.DataIntegration.Integration.Interfaces;
using System;
using System.Collections.Generic;
using System.Data.SqlClient;
using System.Linq;

namespace Dynamicweb.DataIntegration.Providers.EcomProvider
{
    class EcomSourceReader : ISourceReader
    {
        protected SqlCommand _command;
        protected SqlDataReader _reader;
        protected Mapping _mapping;

        private Dictionary<string, ColumnMapping> Columns = new Dictionary<string, ColumnMapping>();
        private Dictionary<string, ColumnMapping> CategoriesColumns = new Dictionary<string, ColumnMapping>();

        private void LoadReaderFromDatabase()
        {
            try
            {
                ColumnMappingCollection columnmappings = _mapping.GetColumnMappings();
                if (columnmappings.Count == 0)
                    return;
                string columns = GetColumns();
                string fromTables = GetFromTables();
                string sql = "select * from (select " + columns + " from  " + fromTables + ") as result";

                List<SqlParameter> parameters = new List<SqlParameter>();
                string conditionalsSql = SqlProvider.MappingExtensions.GetConditionalsSql(out parameters, _mapping.Conditionals, false, false);
                if (conditionalsSql != "")
                {
                    conditionalsSql = conditionalsSql.Substring(0, conditionalsSql.Length - 4);
                    sql = sql + " where " + conditionalsSql;
                    foreach (SqlParameter p in parameters)
                        _command.Parameters.Add(p);
                }
                _command.CommandText = sql;
                _reader = _command.ExecuteReader();
            }
            catch (Exception ex)
            {
                throw new Exception("Failed to open sqlSourceReader. Reason: " + ex.Message, ex);
            }
        }


        protected string GetDistinctColumnsFromMapping()
        {
            return GetDistinctColumnsFromMapping(new HashSet<string> { });
        }

        protected string GetDistinctColumnsFromMapping(HashSet<string> columnsToSkip)
        {
            var result = string.Empty;
            foreach (var mapping in Columns.Keys)
            {
                if (columnsToSkip.Count < 1 || !columnsToSkip.Contains(mapping))
                {
                    result += "[" + mapping + "], ";
                }
            }
            return result;
        }

        protected string GetColumnsFromMappingConditions()
        {
            return GetColumnsFromMappingConditions(new HashSet<string>());
        }

        protected string GetColumnsFromMappingConditions(HashSet<string> columnsToSkip)
        {
            string ret = string.Empty;
            if (_mapping.Conditionals.Count > 0)
            {
                foreach (MappingConditional mc in _mapping.Conditionals.Where(mc => mc != null && mc.SourceColumn != null).GroupBy(g => new { g.SourceColumn.Name }).Select(g => g.First()))
                {
                    if (!columnsToSkip.Contains(mc.SourceColumn.Name.ToLower()) && !Columns.ContainsKey(mc.SourceColumn.Name.ToLower()))
                    {
                        ret += "[" + mc.SourceColumn.Name + "], ";
                    }
                }
            }
            return ret;
        }

        protected bool IsColumnUsedInMappingConditions(string columnName)
        {
            return _mapping.Conditionals.Any(mc => string.Compare(mc.SourceColumn?.Name, columnName, true) == 0);
        }

        public virtual bool IsDone()
        {
            if (_reader.Read())
                return false;
            _reader.Close();
            return true;
        }

        /// <summary>
        /// base implementation, 
        /// </summary>
        /// <returns></returns>
        public virtual Dictionary<string, object> GetNext()
        {
            var rowValues = new Dictionary<string, object>(StringComparer.OrdinalIgnoreCase);
            foreach (var column in Columns.Keys)
            {
                rowValues[column] = _reader[column];
            }
            return rowValues;
        }

        public void Dispose()
        {
            _reader.Close();
        }

        public EcomSourceReader(Mapping mapping, SqlConnection connection, bool getGroupNamesForVariantOptions, bool getManufacturerNamesForProducts, bool getGroupNamesForProduct, bool getVariantGroupNamesForProduct, bool getRelatedProductsByName, bool getRelatedProductGroupsByName)
        {
            this.getGroupNamesForVariantOptions = getGroupNamesForVariantOptions;
            this.getRelatedProductGroupsByName = getRelatedProductGroupsByName;
            this.getRelatedProductsByName = getRelatedProductsByName;
            this.getVariantGroupNamesForProduct = getVariantGroupNamesForProduct;
            this.getGroupNamesForProduct = getGroupNamesForProduct;
            this.getManufacturerNamesForProducts = getManufacturerNamesForProducts;
            DoInitialization(mapping, connection);
        }

        private readonly bool getGroupNamesForVariantOptions;
        private readonly bool getManufacturerNamesForProducts;
        private readonly bool getGroupNamesForProduct;
        private readonly bool getVariantGroupNamesForProduct;
        private readonly bool getRelatedProductsByName;
        private readonly bool getRelatedProductGroupsByName;

        protected void DoInitialization(Mapping mapping, SqlConnection connection)
        {
            foreach (var columnMapping in mapping.GetColumnMappings())
            {
                if (columnMapping.SourceColumn != null)
                {
                    if (!Columns.ContainsKey(columnMapping.SourceColumn.Name.ToLower()))
                    {
                        Columns.Add(columnMapping.SourceColumn.Name.ToLower(), columnMapping);
                    }
                    if (columnMapping.SourceColumn.Name.StartsWith("ProductCategory|"))
                    {
                        if (!CategoriesColumns.ContainsKey(columnMapping.SourceColumn.Name.ToLower()))
                        {
                            CategoriesColumns.Add(columnMapping.SourceColumn.Name.ToLower(), columnMapping);
                        }
                    }
                }
            }
            this._mapping = mapping;
            _command = new SqlCommand { Connection = connection };

            int _commandtimeout = Dynamicweb.Configuration.SystemConfiguration.Instance.Contains("/Globalsettings/Settings/DataIntegration/SQLSourceCommandTimeout") ?
                Converter.ToInt32(Dynamicweb.Configuration.SystemConfiguration.Instance.GetValue("/Globalsettings/Settings/DataIntegration/SQLSourceCommandTimeout")) :
                Converter.ToInt32(Dynamicweb.Configuration.SystemConfiguration.Instance.GetValue("/Globalsettings/DataIntegration/SQLSourceCommandTimeout"));
            if (_commandtimeout > 0)
                _command.CommandTimeout = _commandtimeout;

            if (connection.State.ToString() != "Open")
                connection.Open();
            LoadReaderFromDatabase();
        }

        protected string GetFromTables()
        {
            string result = "[" + _mapping.SourceTable.SqlSchema + "].[" + _mapping.SourceTable.Name +
                             "] as outer" + _mapping.SourceTable.Name;

            switch (_mapping.SourceTable.Name)
            {
                case "EcomGroups":
                    if (Columns.ContainsKey("GroupLanguageID".ToLower()) || IsColumnUsedInMappingConditions("GroupLanguageID"))
                    {
                        result = result + " join EcomLanguages on GroupLanguageID=LanguageID";
                    }
                    break;
                case "EcomVariantGroups":
                    if (Columns.ContainsKey("VariantGroupLanguageID".ToLower()) || IsColumnUsedInMappingConditions("VariantGroupLanguageID"))
                    {
                        result = result + " join EcomLanguages on VariantGroupLanguageID=LanguageID";
                    }
                    break;
                case "EcomVariantsOptions":
                    result = result + " join EcomLanguages on VariantOptionLanguageID=LanguageID";
                    if (getGroupNamesForVariantOptions && Columns.ContainsKey("VariantOptionGroupID".ToLower()))
                    {

                        result = result + " left join EcomVariantGroups on VariantOptionGroupID=VariantGroupID and VariantOptionLanguageID=VariantGroupLanguageID";
                    }
                    break;
                case "EcomProducts":
                    if (Columns.ContainsKey("ProductLanguageID".ToLower()) || IsColumnUsedInMappingConditions("ProductLanguageID"))
                    {
                        result = result + " join EcomLanguages on ProductLanguageID=LanguageID";
                    }
                    if (getManufacturerNamesForProducts && Columns.ContainsKey("ProductManufacturerID".ToLower()))
                    {
                        result = result + " left join EcomManufacturers on productManufacturerID=ManufacturerID";
                    }

                    if (CategoriesColumns.Keys.Any())
                    {
                        result = result + string.Format(@" left join 
                            (SELECT FieldValueProductId, FieldValueProductLanguageId, FieldValueProductVariantId, 
                            {0}
                            FROM (
                                    SELECT FieldValueProductId, FieldValueProductLanguageId, FieldValueProductVariantId, FieldValueValue,
                                        CONCAT ( 'ProductCategory|', [FieldValueFieldCategoryId], '|', [FieldValueFieldId]) as fieldKey
                                    FROM [EcomProductCategoryFieldValue]
                                ) AS SourceTable
                            PIVOT (max(FieldValueValue) FOR fieldKey IN ({0})) AS PivotTable  
		                ) AS categoryValues
                        ON ProductId = FieldValueProductId 
                        AND ProductLanguageId = FieldValueProductLanguageId 
                        AND ProductVariantId = FieldValueProductVariantId ", string.Join(",", CategoriesColumns.Keys.Select(x => $"[{x}]")));
                    }

                    break;
                case "EcomProductsRelated":
                    if (getRelatedProductsByName)
                        result = result +
                                 " join EcomProducts as source on ProductRelatedProductID=source.productID join EcomProducts as destination on ProductRelatedProductRelID=destination.productID";
                    result = result + " join EcomProductsRelatedGroups on ProductRelatedGroupID=RelatedGroupID join EcomLanguages on LanguageID = RelatedGroupLanguageId";
                    break;
                case "EcomProductCategoryFieldValue":
                    result = result +
                             " join EcomProducts on FieldValueProductId=productID and FieldValueProductVariantId=productVariantID and FieldValueProductLanguageId=productLanguageID ";
                    break;
                case "EcomAssortmentPermissions":
                    if (Columns.ContainsKey("AssortmentPermissionAccessUserID".ToLower()) ||
                        Columns.ContainsKey("AssortmentPermissionCustomerNumber".ToLower()) ||
                        Columns.ContainsKey("AssortmentPermissionExternalID".ToLower()))
                    {
                        result = result + " join AccessUser on AssortmentPermissionAccessUserID=AccessUserID";
                    }
                    break;
                default:
                    result = "[" + _mapping.SourceTable.SqlSchema + "].[" + _mapping.SourceTable.Name + "]";
                    if (_mapping.SourceTable != null && _mapping.SourceTable.Name == "EcomAssortmentPermissions" &&
                        (_mapping.GetColumnMappings().Find(cm => cm.SourceColumn != null && cm.SourceColumn.Name.ToLower() == "AssortmentPermissionAccessUserID".ToLower()) != null ||
                        _mapping.GetColumnMappings().Find(cm => cm.SourceColumn != null && cm.SourceColumn.Name.ToLower() == "AssortmentPermissionCustomerNumber".ToLower()) != null ||
                        _mapping.GetColumnMappings().Find(cm => cm.SourceColumn != null && cm.SourceColumn.Name.ToLower() == "AssortmentPermissionExternalID".ToLower()) != null))
                    {
                        result = "[" + _mapping.SourceTable.SqlSchema + "].[" + _mapping.SourceTable.Name + "] as outer" + _mapping.SourceTable.Name;
                        result = result + " join AccessUser on AssortmentPermissionAccessUserID=AccessUserID";
                    }
                    break;
            }
            return result;
        }

        protected string GetColumns()
        {
            string result = "";
            switch (_mapping.SourceTable.Name)
            {
                case "EcomGroups":
                    var ecomGroupfieldsToSkip = new HashSet<string> { "shops", "grouplanguageid", "parentgroups", "shopsorting", "parentgroupssorting" };
                    result = GetDistinctColumnsFromMapping(ecomGroupfieldsToSkip);
                    if (Columns.ContainsKey("shops") || IsColumnUsedInMappingConditions("Shops"))
                    {
                        result = result + "STUFF((SELECT ',\"'+ ShopGroupShopID +'\"' FROM ecomgroups JOIN EcomShopGroupRelation on groupid=ShopGroupGroupID WHERE GroupID=outerEcomGroups.GroupID FOR XML PATH('')),1,1,'')  as Shops, ";
                    }
                    if (Columns.ContainsKey("shopsorting") || IsColumnUsedInMappingConditions("ShopSorting"))
                    {
                        result = result + "STUFF((SELECT ',\"'+  convert(nvarchar,ShopGroupRelationsSorting) +'\"' FROM ecomgroups JOIN EcomShopGroupRelation on groupid=ShopGroupGroupID WHERE GroupID=outerEcomGroups.GroupID FOR XML PATH('')),1,1,'')  as ShopSorting, ";
                    }
                    if ((Columns.ContainsKey("grouplanguageid")) || IsColumnUsedInMappingConditions("GroupLanguageID"))
                    {
                        result = result + "[LanguageID] as GroupLanguageID, ";
                    }
                    if (Columns.ContainsKey("parentgroups") || IsColumnUsedInMappingConditions("ParentGroups"))
                    {
                        result = result + "STUFF((SELECT ',\"'+ GroupRelationsParentID+'\"' FROM ecomgroups JOIN EcomGroupRelations on groupid=GroupRelationsGroupID WHERE GroupID=outerEcomGroups.GroupID FOR XML PATH('')),1,1,'')  as ParentGroups, ";
                    }
                    if (Columns.ContainsKey("parentgroupssorting") || IsColumnUsedInMappingConditions("ParentGroupsSorting"))
                    {
                        result = result + "STUFF((SELECT ',\"'+ convert(nvarchar,GroupRelationsSorting)+'\"' FROM ecomgroups JOIN EcomGroupRelations on groupid=GroupRelationsGroupID WHERE GroupID=outerEcomGroups.GroupID FOR XML PATH('')),1,1,'')  as ParentGroupsSorting, ";
                    }
                    result += GetColumnsFromMappingConditions(ecomGroupfieldsToSkip);
                    break;
                case "EcomVariantGroups":
                    var ecomVariantGroupfieldsToSkip = new HashSet<string> { "variantgrouplanguageid" };
                    result = GetDistinctColumnsFromMapping(ecomVariantGroupfieldsToSkip);

                    if ((Columns.ContainsKey("VariantGroupLanguageID".ToLower())) ||
                        IsColumnUsedInMappingConditions("VariantGroupLanguageID"))
                    {
                        result = result + "[LanguageID] as VariantGroupLanguageID, ";
                    }
                    result += GetColumnsFromMappingConditions(ecomVariantGroupfieldsToSkip);
                    break;
                case "EcomVariantsOptions":
                    var ecomVariantsOptionsfieldsToSkip = new HashSet<string> { "variantoptionlanguageid", "variantoptiongroupid" };
                    result = GetDistinctColumnsFromMapping(ecomVariantsOptionsfieldsToSkip);
                    if ((Columns.ContainsKey("VariantOptionLanguageID".ToLower())) ||
                        IsColumnUsedInMappingConditions("VariantOptionLanguageID"))
                    {
                        result = result + "[LanguageID] as VariantOptionLanguageID, ";
                    }
                    if ((Columns.ContainsKey("VariantOptionGroupID".ToLower())) ||
                        IsColumnUsedInMappingConditions("VariantOptionGroupID"))
                    {
                        if (getGroupNamesForVariantOptions)
                            result = result + "[VariantGroupName] as VariantOptionGroupID, ";
                        else
                        {
                            result = result + "[VariantOptionGroupID], ";
                        }
                    }
                    result += GetColumnsFromMappingConditions(ecomVariantsOptionsfieldsToSkip);
                    break;
                case "EcomProducts":
                    var ecomProductfieldsToSkip = new HashSet<string> { "groups", "groupsorting", "primarygroup", "variantgroups", "productlanguageid", "productmanufacturerid", "variantoptions", "relatedproducts" };
                    result = GetDistinctColumnsFromMapping(ecomProductfieldsToSkip);
                    if (Columns.ContainsKey("Groups".ToLower()) || IsColumnUsedInMappingConditions("Groups"))
                    {
                        if (getGroupNamesForProduct)
                            result = result + " STUFF((SELECT ',\"'+ groupname +'\"' FROM EcomProducts JOIN EcomGroupProductRelation on ProductID=GroupProductRelationProductID JOIN ecomGroups on GroupID=GroupProductRelationGroupID WHERE ProductID=outerEcomProducts.ProductID and ProductLanguageID=outerEcomProducts.ProductLanguageID and ProductVariantID=outerEcomProducts.ProductVariantID and GroupLanguageID=ProductLanguageID FOR XML PATH('')),1,1,'') as Groups, ";
                        else
                            result = result + "STUFF((SELECT ',\"'+ GroupID +'\"' FROM EcomProducts JOIN EcomGroupProductRelation on ProductID=GroupProductRelationProductID JOIN ecomGroups on GroupID=GroupProductRelationGroupID WHERE ProductID=outerEcomProducts.ProductID and ProductLanguageID=outerEcomProducts.ProductLanguageID and ProductVariantID=outerEcomProducts.ProductVariantID and GroupLanguageID=ProductLanguageID FOR XML PATH('')),1,1,'') as Groups, ";
                    }
                    if (Columns.ContainsKey("GroupSorting".ToLower()) || IsColumnUsedInMappingConditions("GroupSorting"))
                    {
                        result = result + "STUFF((SELECT ',\"'+ convert(nvarchar,GroupProductRelationSorting)+'\"' FROM EcomProducts JOIN EcomGroupProductRelation on ProductID=GroupProductRelationProductID JOIN ecomGroups on GroupID=GroupProductRelationGroupID WHERE ProductID=outerEcomProducts.ProductID and ProductLanguageID=outerEcomProducts.ProductLanguageID and ProductVariantID=outerEcomProducts.ProductVariantID and GroupLanguageID=ProductLanguageID FOR XML PATH('')),1,1,'') as GroupSorting, ";
                    }
                    if (Columns.ContainsKey("PrimaryGroup".ToLower()) || IsColumnUsedInMappingConditions("PrimaryGroup"))
                    {
                        result = result + "(SELECT TOP(1) GroupID FROM EcomProducts JOIN EcomGroupProductRelation on ProductID=GroupProductRelationProductID JOIN ecomGroups on GroupID=GroupProductRelationGroupID WHERE ProductID=outerEcomProducts.ProductID and ProductLanguageID=outerEcomProducts.ProductLanguageID and ProductVariantID=outerEcomProducts.ProductVariantID and GroupLanguageID=ProductLanguageID and GroupProductRelationIsPrimary=1) as PrimaryGroup, ";
                    }
                    if (Columns.ContainsKey("VariantGroups".ToLower()) || IsColumnUsedInMappingConditions("VariantGroups"))
                    {
                        if (getVariantGroupNamesForProduct)
                        {
                            result = result + "STUFF((SELECT ',\"'+ VariantGroupName +'\"' FROM EcomProducts JOIN EcomVariantgroupProductRelation on ProductID=VariantgroupProductRelationProductID JOIN EcomVariantGroups on VariantGroupID=VariantgroupProductRelationVariantGroupID WHERE ProductID=outerEcomProducts.ProductID and ProductLanguageID=outerEcomProducts.ProductLanguageID and ProductVariantID=outerEcomProducts.ProductVariantID and variantGroupLanguageID=ProductLanguageID FOR XML PATH('')),1,1,'') as VariantGroups, ";
                        }
                        else
                        {
                            result = result + "STUFF((SELECT ',\"'+ VariantGroupid +'\"' FROM EcomProducts JOIN EcomVariantgroupProductRelation on ProductID=VariantgroupProductRelationProductID JOIN EcomVariantGroups on VariantGroupID=VariantgroupProductRelationVariantGroupID WHERE ProductID=outerEcomProducts.ProductID and ProductLanguageID=outerEcomProducts.ProductLanguageID and ProductVariantID=outerEcomProducts.ProductVariantID and variantGroupLanguageID=ProductLanguageID FOR XML PATH('')),1,1,'')  as VariantGroups, ";
                        }
                    }
                    if ((Columns.ContainsKey("ProductLanguageID".ToLower())) || IsColumnUsedInMappingConditions("ProductLanguageID"))
                    {
                        result = result + "[LanguageID] as ProductLanguageID, ";

                    }
                    if ((Columns.ContainsKey("ProductManufacturerID".ToLower())) || IsColumnUsedInMappingConditions("ProductManufacturerID"))
                    {
                        if (getManufacturerNamesForProducts)
                        {
                            result = result + "isnull([ManufacturerName],'') as ProductManufacturerId, ";
                        }
                        else
                        {
                            result = result + "[ProductManufacturerId], ";
                        }
                    }
                    if (Columns.ContainsKey("VariantOptions".ToLower()) || IsColumnUsedInMappingConditions("VariantOptions"))
                    {
                        result = result + "STUFF((SELECT ',\"'+ DistinctVariants.VariantOptionsProductRelationVariantID +'\"' FROM ";
                        result += "(SELECT DISTINCT VariantOptionsProductRelationVariantID FROM EcomProducts JOIN EcomVariantOptionsProductRelation on ProductID=VariantOptionsProductRelationProductID WHERE ProductID=outerEcomProducts.ProductID) DistinctVariants FOR XML PATH('')),1,1,'')  as VariantOptions, ";
                    }
                    if (Columns.ContainsKey("RelatedProducts".ToLower()) || IsColumnUsedInMappingConditions("RelatedProducts"))
                    {
                        if (getRelatedProductsByName)
                        {
                            result = result + "STUFF((SELECT ',\"'+ ProductName+'\"' FROM EcomProducts JOIN EcomProductsRelated on ProductID=ProductRelatedProductRelID WHERE productrelatedProductID=outerEcomProducts.ProductID FOR XML PATH('')),1,1,'')  as RelatedProducts, ";
                        }
                        else
                        {
                            result = result + "STUFF((SELECT ',\"'+ ProductID+'\"' FROM EcomProducts JOIN EcomProductsRelated on ProductID=ProductRelatedProductRelID WHERE productrelatedProductID=outerEcomProducts.ProductID FOR XML PATH('')),1,1,'')  as RelatedProducts, ";
                        }
                    }
                    result += GetColumnsFromMappingConditions(ecomProductfieldsToSkip);
                    break;
                case "EcomProductsRelated":
                    result = GetDistinctColumnsFromMapping(new HashSet<string> { "productrelatedproductid", "productrelatedproductrelid", "productrelatedgroupid", "productrelatedlanguageid" });
                    if (getRelatedProductsByName)
                    {
                        result = result + "source.ProductName as ProductRelatedProductID,  destination.productName as ProductRelatedProductRelID, ";
                    }
                    else
                    {
                        result = result + "ProductRelatedProductID, ProductRelatedProductRelID, ";
                    }
                    if (getRelatedProductGroupsByName)
                    {
                        result = result + "RelatedGroupName as ProductrelatedGroupID, ";
                    }
                    else
                    {
                        result = result + "ProductrelatedGroupID, ";
                    }
                    if ((Columns.ContainsKey("ProductRelatedLanguageID".ToLower())) || IsColumnUsedInMappingConditions("ProductRelatedLanguageID"))
                    {
                        result = result + "LanguageID as ProductRelatedLanguageID, ";
                    }
                    result += GetColumnsFromMappingConditions(new HashSet<string> { "productrelatedlanguageid" });
                    break;
                case "EcomProductCategoryFieldValue":
                    var ecomProductCategoryFieldValuefieldsToSkip = new HashSet<string> { "fieldvalueproductnumber" };
                    result = GetDistinctColumnsFromMapping(ecomProductCategoryFieldValuefieldsToSkip);
                    if (Columns.ContainsKey("FieldValueProductNumber".ToLower()) || IsColumnUsedInMappingConditions("FieldValueProductNumber"))
                    {
                        result = result + "[ProductNumber] as FieldValueProductNumber, ";
                    }
                    result += GetColumnsFromMappingConditions(ecomProductCategoryFieldValuefieldsToSkip);
                    break;
                case "EcomAssortmentPermissions":
                    result = GetDistinctColumnsFromMapping(new HashSet<string> { "assortmentpermissioncustomernumber", "assortmentpermissionexternalid" });
                    if (Columns.ContainsKey("AssortmentPermissionCustomerNumber".ToLower()) || IsColumnUsedInMappingConditions("AssortmentPermissionCustomerNumber"))
                    {
                        result = result + "(SELECT AccessUserCustomerNumber FROM AccessUser JOIN EcomAssortmentPermissions on AssortmentPermissionAccessUserID=AccessUserID WHERE AccessUserID=outerEcomAssortmentPermissions.AssortmentPermissionAccessUserID) as AssortmentPermissionCustomerNumber, ";
                    }
                    if (Columns.ContainsKey("AssortmentPermissionExternalID".ToLower()))
                    {
                        result = result + "(SELECT AccessUserExternalID FROM AccessUser JOIN EcomAssortmentPermissions on AssortmentPermissionAccessUserID=AccessUserID WHERE AccessUserID=outerEcomAssortmentPermissions.AssortmentPermissionAccessUserID) as AssortmentPermissionExternalID, ";
                    }
                    result += GetColumnsFromMappingConditions(new HashSet<string> { "assortmentpermissioncustomernumber", "assortmentpermissionexternalid" });
                    break;
                default:
                    result = GetDistinctColumnsFromMapping();

                    if (_mapping.SourceTable != null && _mapping.SourceTable.Name == "EcomAssortmentPermissions")
                    {
                        result = GetDistinctColumnsFromMapping(new HashSet<string> { "assortmentpermissioncustomernumber", "assortmentpermissionexternalid" });
                        if (Columns.ContainsKey("AssortmentPermissionCustomerNumber".ToLower()))
                        {
                            result = result + "(SELECT AccessUserCustomerNumber FROM AccessUser JOIN EcomAssortmentPermissions on AssortmentPermissionAccessUserID=AccessUserID WHERE AccessUserID=outerEcomAssortmentPermissions.AssortmentPermissionAccessUserID) as AssortmentPermissionCustomerNumber, ";
                        }
                        if (Columns.ContainsKey("AssortmentPermissionExternalID".ToLower()))
                        {
                            result = result + "(SELECT AccessUserExternalID FROM AccessUser JOIN EcomAssortmentPermissions on AssortmentPermissionAccessUserID=AccessUserID WHERE AccessUserID=outerEcomAssortmentPermissions.AssortmentPermissionAccessUserID) as AssortmentPermissionExternalID, ";
                        }
                    }
                    result += GetColumnsFromMappingConditions();
                    break;
            }
            result = result.Substring(0, result.Length - 2);
            return result;
        }
    }
}
