package io.datio.connect.file.filesys;

//import java.sql.FilesysException;
//import java.sql.Types;




/**
 * An object that can be used to get information about the types
 * and properties of the columns in a <code>ResultSet</code> object.
 * The following code fragment creates the <code>ResultSet</code> object rs,
 * creates the <code>ResultSetMetaData</code> object rsmd, and uses rsmd
 * to find out how many columns rs has and whether the first column in rs
 * can be used in a <code>WHERE</code> clause.
 * <PRE>
 *
 *     ResultSet rs = stmt.executeQuery("SELECT a, b, c FROM TABLE2");
 *     ResultSetMetaData rsmd = rs.getMetaData();
 *     int numberOfColumns = rsmd.getColumnCount();
 *     boolean b = rsmd.isSearchable(1);
 *
 * </PRE>
 */

public interface FilesysEntityMetadata extends MetadataWrapper {

    /**
     * Returns the number of columns in this <code>ResultSet</code> object.
     *
     * @return the number of columns
     * @exception FilesysException if a database access error occurs
     */
    int getColumnCount() throws FilesysException;

    /**
     * Indicates whether the designated column is automatically numbered.
     *
     * @param column the first column is 1, the second is 2, ...
     * @return <code>true</code> if so; <code>false</code> otherwise
     * @exception FilesysException if a database access error occurs
     */
    boolean isAutoIncrement(int column) throws FilesysException;

    /**
     * Indicates whether a column's case matters.
     *
     * @param column the first column is 1, the second is 2, ...
     * @return <code>true</code> if so; <code>false</code> otherwise
     * @exception FilesysException if a database access error occurs
     */
    boolean isCaseSensitive(int column) throws FilesysException;

    /**
     * Indicates whether the designated column can be used in a where clause.
     *
     * @param column the first column is 1, the second is 2, ...
     * @return <code>true</code> if so; <code>false</code> otherwise
     * @exception FilesysException if a database access error occurs
     */
    boolean isSearchable(int column) throws FilesysException;

    /**
     * Indicates whether the designated column is a cash value.
     *
     * @param column the first column is 1, the second is 2, ...
     * @return <code>true</code> if so; <code>false</code> otherwise
     * @exception FilesysException if a database access error occurs
     */
    boolean isCurrency(int column) throws FilesysException;

    /**
     * Indicates the nullability of values in the designated column.
     *
     * @param column the first column is 1, the second is 2, ...
     * @return the nullability status of the given column; one of <code>columnNoNulls</code>,
     *          <code>columnNullable</code> or <code>columnNullableUnknown</code>
     * @exception FilesysException if a database access error occurs
     */
    int isNullable(int column) throws FilesysException;

    /**
     * The constant indicating that a
     * column does not allow <code>NULL</code> values.
     */
    int columnNoNulls = 0;

    /**
     * The constant indicating that a
     * column allows <code>NULL</code> values.
     */
    int columnNullable = 1;

    /**
     * The constant indicating that the
     * nullability of a column's values is unknown.
     */
    int columnNullableUnknown = 2;

    /**
     * Indicates whether values in the designated column are signed numbers.
     *
     * @param column the first column is 1, the second is 2, ...
     * @return <code>true</code> if so; <code>false</code> otherwise
     * @exception FilesysException if a database access error occurs
     */
    boolean isSigned(int column) throws FilesysException;

    /**
     * Indicates the designated column's normal maximum width in characters.
     *
     * @param column the first column is 1, the second is 2, ...
     * @return the normal maximum number of characters allowed as the width
     *          of the designated column
     * @exception FilesysException if a database access error occurs
     */
    int getColumnDisplaySize(int column) throws FilesysException;

    /**
     * Gets the designated column's suggested title for use in printouts and
     * displays. The suggested title is usually specified by the SQL <code>AS</code>
     * clause.  If a SQL <code>AS</code> is not specified, the value returned from
     * <code>getColumnLabel</code> will be the same as the value returned by the
     * <code>getColumnName</code> method.
     *
     * @param column the first column is 1, the second is 2, ...
     * @return the suggested column title
     * @exception FilesysException if a database access error occurs
     */
    String getColumnLabel(int column) throws FilesysException;

    /**
     * Get the designated column's name.
     *
     * @param column the first column is 1, the second is 2, ...
     * @return column name
     * @exception FilesysException if a database access error occurs
     */
    String getColumnName(int column) throws FilesysException;

    /**
     * Get the designated column's table's schema.
     *
     * @param column the first column is 1, the second is 2, ...
     * @return schema name or "" if not applicable
     * @exception FilesysException if a database access error occurs
     */
    String getSchemaName(int column) throws FilesysException;

    /**
     * Get the designated column's specified column size.
     * For numeric data, this is the maximum precision.  For character data, this is the length in characters.
     * For datetime datatypes, this is the length in characters of the String representation (assuming the
     * maximum allowed precision of the fractional seconds component). For binary data, this is the length in bytes.  For the ROWID datatype,
     * this is the length in bytes. 0 is returned for data types where the
     * column size is not applicable.
     *
     * @param column the first column is 1, the second is 2, ...
     * @return precision
     * @exception FilesysException if a database access error occurs
     */
    int getPrecision(int column) throws FilesysException;

    /**
     * Gets the designated column's number of digits to right of the decimal point.
     * 0 is returned for data types where the scale is not applicable.
     *
     * @param column the first column is 1, the second is 2, ...
     * @return scale
     * @exception FilesysException if a database access error occurs
     */
    int getScale(int column) throws FilesysException;

    /**
     * Gets the designated column's table name.
     *
     * @param column the first column is 1, the second is 2, ...
     * @return table name or "" if not applicable
     * @exception FilesysException if a database access error occurs
     */
    String getTableName(int column) throws FilesysException;

    /**
     * Gets the designated column's table's catalog name.
     *
     * @param column the first column is 1, the second is 2, ...
     * @return the name of the catalog for the table in which the given column
     *          appears or "" if not applicable
     * @exception FilesysException if a database access error occurs
     */
    String getCatalogName(int column) throws FilesysException;

    /**
     * Retrieves the designated column's SQL type.
     *
     * @param column the first column is 1, the second is 2, ...
     * @return SQL type from java.sql.Types
     * @exception FilesysException if a database access error occurs

     */
    int getColumnType(int column) throws FilesysException;

    /**
     * Retrieves the designated column's database-specific type name.
     *
     * @param column the first column is 1, the second is 2, ...
     * @return type name used by the database. If the column type is
     * a user-defined type, then a fully-qualified type name is returned.
     * @exception FilesysException if a database access error occurs
     */
    String getColumnTypeName(int column) throws FilesysException;

    /**
     * Indicates whether the designated column is definitely not writable.
     *
     * @param column the first column is 1, the second is 2, ...
     * @return <code>true</code> if so; <code>false</code> otherwise
     * @exception FilesysException if a database access error occurs
     */
    boolean isReadOnly(int column) throws FilesysException;

    /**
     * Indicates whether it is possible for a write on the designated column to succeed.
     *
     * @param column the first column is 1, the second is 2, ...
     * @return <code>true</code> if so; <code>false</code> otherwise
     * @exception FilesysException if a database access error occurs
     */
    boolean isWritable(int column) throws FilesysException;

    /**
     * Indicates whether a write on the designated column will definitely succeed.
     *
     * @param column the first column is 1, the second is 2, ...
     * @return <code>true</code> if so; <code>false</code> otherwise
     * @exception FilesysException if a database access error occurs
     */
    boolean isDefinitelyWritable(int column) throws FilesysException;

    //--------------------------JDBC 2.0-----------------------------------


    String getColumnClassName(int column) throws FilesysException;

    String getIdentifierQuoteString();


    FileRegexResultset getColumns(Object o, Object o1, String table, String s);
}
