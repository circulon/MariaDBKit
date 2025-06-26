//
//  MariaDBResultSet.m
//  MariaDBKit
//
//  Created by Kyle Hankinson on 2019-03-06.
//  Copyright © 2019 Kyle Hankinson. All rights reserved.
//

#import "MariaDBResultSet.h"
#import "MariaDBResultSetPrivate.h"

@interface MariaDBResultSet ()
{
    unsigned long long  _totalRows;
    unsigned long long  _currentRowIndex;
    NSUInteger          _totalFields;
    
    MYSQL_RES           * _internalMySQLResult;
    MYSQL_ROW           _internalMySQLRow;
    
    MYSQL_FIELD         * _internalFields;
    
    NSNumberFormatter   * _numberFormatter;
    NSDataDetector      * _dateDetector;
}

@property(nonatomic,copy) NSArray * columnNames;
@property(nonatomic,copy) NSArray * columnTypes;

@end

@implementation MariaDBResultSet
{
    NSArray * _currentRowFieldLengths;
}

@synthesize columnNames, columnTypes;

- (id) initWithResult: (MYSQL_RES*) result
{
    self = [super init];
    if(!self) { return nil; }
    // The number formatter
    _numberFormatter = [[NSNumberFormatter alloc] init];
    [_numberFormatter setNumberStyle: NSNumberFormatterDecimalStyle];
    
    // Data detector
    _dateDetector = [NSDataDetector dataDetectorWithTypes: NSTextCheckingTypeDate
                                                   error: nil];
    
    _internalMySQLResult = result;
    if(NULL == _internalMySQLResult) { return self; }

    _totalFields = mysql_num_fields(_internalMySQLResult);
    _totalRows   = mysql_num_rows(_internalMySQLResult);
    _currentRowIndex = 0;

    _internalFields = mysql_fetch_fields(_internalMySQLResult);
    
    NSMutableArray * _columnNames  = [NSMutableArray array];
    NSMutableArray * _columnTypes = [NSMutableArray array];
    NSMutableArray * _charSets    = [NSMutableArray array];
    
    for(int i = 0; i < _totalFields; i++)
    {
        [_columnNames addObject: [NSString stringWithUTF8String: _internalFields[i].name]];
        [_columnTypes addObject: [NSNumber numberWithInt: _internalFields[i].type]];
        [_charSets addObject: [NSNumber numberWithInt: _internalFields[i].charsetnr]];
    } // End of finished
    
    // Get our field names
    columnNames         = _columnNames.copy;
    columnTypes         = _columnTypes.copy;
    
    if (_totalRows > 0) {
        // load the first row
        [self _loadRowAtIndex:0];
    }
    
    return self;
} // End of init

- (void) dealloc
{
    if(NULL != _internalMySQLResult)
    {
        _internalMySQLRow = NULL;
        mysql_free_result(_internalMySQLResult);
        _internalMySQLResult = NULL;
    } // End of clear the result set
} // End of dealloc

- (NSUInteger) rowCount
{
    return (NSUInteger)_totalRows;
}

- (NSArray<NSString*>*) columnNames
{
    return columnNames;
}

- (NSUInteger)columnCount
{
    return _totalFields;
}

- (NSString*)columnNameForIndex:(int)columnIdx
{
    return columnNames[columnIdx];
}

- (BOOL)columnIsNull:(NSString *)columnName{
    // Get our columnIndex
    NSUInteger columnIndex = [columnNames indexOfObject: columnName];
                
    return [self columnAtIndexIsNull:columnIndex];
}

- (BOOL)columnAtIndexIsNull:(NSInteger)columnIndex{
    return (NULL == _internalMySQLRow[columnIndex]);
}

- (id) objectForColumn: (NSString*) columnName
{
    // Get our columnIndex
    NSUInteger columnIndex = [columnNames indexOfObject: columnName];
    
    NSAssert1(NSNotFound != columnIndex, @"Column %@ could not be found.", columnName);
    
    return [self objectForColumnIndex: columnIndex];
}

- (id) objectForColumnIndex: (NSUInteger) columnIndex
{
    /*
     enum enum_field_types { MYSQL_TYPE_DECIMAL, MYSQL_TYPE_TINY,
     MYSQL_TYPE_SHORT,  MYSQL_TYPE_LONG,
     MYSQL_TYPE_FLOAT,  MYSQL_TYPE_DOUBLE,
     MYSQL_TYPE_NULL,   MYSQL_TYPE_TIMESTAMP,
     MYSQL_TYPE_LONGLONG,MYSQL_TYPE_INT24,
     MYSQL_TYPE_DATE,   MYSQL_TYPE_TIME,
     MYSQL_TYPE_DATETIME, MYSQL_TYPE_YEAR,
     MYSQL_TYPE_NEWDATE, MYSQL_TYPE_VARCHAR,
     MYSQL_TYPE_BIT,
     MYSQL_TYPE_NEWDECIMAL=246,
     MYSQL_TYPE_ENUM=247,
     MYSQL_TYPE_SET=248,
     MYSQL_TYPE_TINY_BLOB=249,
     MYSQL_TYPE_MEDIUM_BLOB=250,
     MYSQL_TYPE_LONG_BLOB=251,
     MYSQL_TYPE_BLOB=252,
     MYSQL_TYPE_VAR_STRING=253,
     MYSQL_TYPE_STRING=254,
     MYSQL_TYPE_GEOMETRY=255,
     MAX_NO_FIELD_TYPES
     */
    //    NSLog(@"Internal row: %s", );
    MYSQL_FIELD currentField = _internalFields[columnIndex];
    
    // No data, then we are a null.
    if(NULL == _internalMySQLRow[columnIndex])
    {
        return [NSNull null];
    }
    
    BOOL isEnum = NO;
    BOOL isSet  = NO;
    
    if((currentField.flags & ENUM_FLAG) == ENUM_FLAG)
    {
        isEnum = YES;
    }
    
    if((currentField.flags & SET_FLAG) == SET_FLAG)
    {
        isSet = YES;
    }
    
    if(isEnum || isSet)
    {
        NSLog(@"Is flag or enum.");
    }
    
    id result = nil;
    switch(currentField.type)
    {
        case MYSQL_TYPE_YEAR:
        case MYSQL_TYPE_INT24:
        case MYSQL_TYPE_TINY:
        case MYSQL_TYPE_SHORT:
        case MYSQL_TYPE_LONG:
        case MYSQL_TYPE_LONGLONG:
        {
            NSString * tempString = [NSString stringWithUTF8String: _internalMySQLRow[columnIndex]];
            result = [_numberFormatter numberFromString: tempString];
            break;
        }
        case MYSQL_TYPE_TIME:
        case MYSQL_TYPE_TIMESTAMP:
        case MYSQL_TYPE_DATETIME:
        case MYSQL_TYPE_DATE:
        {
            NSString * dateString = [NSString stringWithUTF8String: _internalMySQLRow[columnIndex]];
            
            NSRange decimalRange = [dateString rangeOfString: @"."
                                                     options: NSBackwardsSearch];
            
            NSUInteger nanoseconds = 0;
            if(NSNotFound != decimalRange.location)
            {
                NSString * nanosecondsString = [dateString substringFromIndex: decimalRange.location + 1];
                nanoseconds = nanosecondsString.integerValue;
            } // End of we have milliseconds
            
            // TempFix -- 0000-00-00 00:00:00 is not handled by dateDetector, so we check
            // and handle it ourself.
            NSString * tempString = [dateString stringByReplacingOccurrencesOfString: @"0"
                                                                          withString: @""];
            
            tempString = [tempString stringByReplacingOccurrencesOfString: @":"
                                                               withString: @""];
            
            tempString = [tempString stringByReplacingOccurrencesOfString: @"-"
                                                               withString: @""];
            
            tempString = [tempString stringByReplacingOccurrencesOfString: @" "
                                                               withString: @""];
            
            if(0 == tempString.length)
            {
                result = dateString;
            }
            else
            {
                __block NSDate * detectedDate = nil;
                [_dateDetector enumerateMatchesInString: dateString
                                               options: kNilOptions
                                                 range: NSMakeRange(0, [dateString length])
                                            usingBlock:^(NSTextCheckingResult *result, NSMatchingFlags flags, BOOL *stop)
                 {
                     detectedDate = result.date;
                 }];
                
                if(nil == detectedDate || 0 != nanoseconds)
                {
                    result = dateString;
                } // End of we have millseconds
                else
                {
                    result = detectedDate;
                }
            }
            
            break;
        }
        case MYSQL_TYPE_LONG_BLOB:
        case MYSQL_TYPE_MEDIUM_BLOB:
        case MYSQL_TYPE_BLOB:
        case MYSQL_TYPE_STRING:
        case MYSQL_TYPE_VAR_STRING:
        case MYSQL_TYPE_JSON:
        {
            if(MYSQL_TYPE_JSON == currentField.type)
            {
                result = [NSString stringWithUTF8String: _internalMySQLRow[columnIndex]];
            }
            else if(63 == currentField.charsetnr)
            {
                result = [NSData dataWithBytes: _internalMySQLRow[columnIndex]
                                        length: [_currentRowFieldLengths[columnIndex] unsignedIntegerValue]];
                
                if(nil != result)
                {
                    NSString * stringResult = [[NSString alloc] initWithData: result
                                                                    encoding: NSUTF8StringEncoding];
                    
                    if(stringResult.length == ((NSData*)result).length)
                    {
                        result = stringResult;
                    }
                }
            }
            else
            {
                result = [NSString stringWithUTF8String: _internalMySQLRow[columnIndex]];
            }
            break;
        }
        case MYSQL_TYPE_BIT:
        {
            if(0 == _internalMySQLRow[columnIndex][0])
            {
                result = [NSNumber numberWithBool: NO];
            }
            else
            {
                result = [NSNumber numberWithBool: YES];
            }
            break;
        }
        case MYSQL_TYPE_FLOAT:
        case MYSQL_TYPE_DECIMAL:
        case MYSQL_TYPE_DOUBLE:
        case MYSQL_TYPE_NEWDECIMAL:
        {
            NSString * stringValue =
            [NSString stringWithUTF8String: _internalMySQLRow[columnIndex]];
            
            NSDecimalNumber * number =
            [NSDecimalNumber decimalNumberWithString: stringValue];
            
            result = number;
            
            break;
        }
        default:
        {
            NSAssert2(false, @"Invalid field type %d (column %@).",
                      _internalFields[columnIndex].type,
                      columnNames[columnIndex]);
            break;
        }
    } // End of data type switch
    
    // If we were unable to set our result, then null it.
    if(nil == result)
    {
        return [NSNull null];
    }
    
    return result;
} // End of objectForColumnIndex


#pragma mark -
#pragma mark Row transforms

- (NSDictionary *)rowAsDictionary
{
    // the query was successful but has no columns
    if ([self columnCount] == 0) {
        return [NSDictionary dictionary];
    }

    NSMutableDictionary *rowDict = [NSMutableDictionary dictionary];
    for (NSString *column in columnNames) {
        NSUInteger columnIndex = [columnNames indexOfObject: column];
        rowDict[column] = [self objectForColumnIndex: columnIndex];
    }
    return (NSDictionary*)[rowDict copy];
}

- (NSArray *)allRows
{
    if (_totalRows == 0) {
        return nil;
    }
    
    NSMutableArray * allRows = [NSMutableArray array];
    // add the current (
    
    // store the current position
    unsigned long long previousRowIndex = _currentRowIndex;
    //reset the position
    [self reset];

    // add the first row
    [allRows addObject:[self rowAsDictionary]];
    // Loop over the remaining rows
    while([self nextRow]){
        [allRows addObject:[self rowAsDictionary]];
    }

    // Seek to the previous position if appropriate
    if (previousRowIndex != _currentRowIndex) {
        _currentRowIndex = previousRowIndex;
        [self _loadRowAtIndex:previousRowIndex];
    }
    
    // Instead of empty arrays, return nil if there are no rows.
    if (![allRows count]) {
        allRows = nil;
        return nil;
    }

    return (NSArray*)[allRows copy];
}
   
- (BOOL)nextRow {
    if (_currentRowIndex >= _totalRows) {
        return NO;
    }

    return [self _loadRowAtIndex:_currentRowIndex++];
}
   
- (BOOL)_loadRowAtIndex:(unsigned long long)index {
    if (index >= _totalRows) {
        return NO;
    }

    // attempt to get the row
    mysql_data_seek(_internalMySQLResult, index);
    _internalMySQLRow = mysql_fetch_row(_internalMySQLResult);
    if (NULL == _internalMySQLRow) {
        return NO;
    }
       
    // store the field lengths for this row
    unsigned long * myLengths = mysql_fetch_lengths(_internalMySQLResult);
    NSMutableArray * outLengths = [NSMutableArray array];
    for(NSUInteger index = 0;
        index < columnNames.count;
        ++index)
    {
        outLengths[index] = [NSNumber numberWithUnsignedLong: myLengths[index]];
    }
    // Set our currentRowField lengths
    _currentRowFieldLengths = outLengths.copy;
    
    return YES;
}

- (void)reset {
    if (_currentRowIndex == 0) { return; }

    _currentRowIndex = 0;
    mysql_data_seek(_internalMySQLResult, 0);
}

#pragma mark -
#pragma mark Field Datatype Transforms

- (NSNumber*) boolForColumn: (NSString*) columnName
{
    // Get our columnIndex
    NSUInteger columnIndex = [columnNames indexOfObject: columnName];
    
    NSAssert1(NSNotFound != columnIndex, @"Column %@ could not be found.", columnName);
    
    return [self boolForColumnIndex: columnIndex];
}

- (NSNumber*) boolForColumnIndex: (NSUInteger) columnIndex
{
    if(nil == _internalMySQLRow[columnIndex])
    {
        return nil;
    }
    
    NSString * tempString = [NSString stringWithUTF8String: _internalMySQLRow[columnIndex]];
    return [NSNumber numberWithBool: [tempString boolValue]];
} // End of boolForColumnIndex

- (NSString*) stringForColumn: (NSString*) columnName
{
    // Get our columnIndex
    NSUInteger columnIndex = [columnNames indexOfObject: columnName];
    
    NSAssert1(NSNotFound != columnIndex, @"Column %@ could not be found.", columnName);
    
    return [self stringForColumnIndex: columnIndex];
}

- (NSString*) stringForColumnIndex: (NSUInteger) columnIndex
{
    if(nil == _internalMySQLRow[columnIndex])
    {
        return nil;
    } // End of entry is nil
    
    id result = [[NSString alloc] initWithUTF8String: _internalMySQLRow[columnIndex]];
    
    if([NSNull null] == result)
    {
        return nil;
    }
    
    return result;
} // End of stringForColumnIndex

- (NSNumber*) numberForColumn: (NSString*) columnName
{
    // Get our columnIndex
    NSUInteger columnIndex = [columnNames indexOfObject: columnName];
    
    NSAssert1(NSNotFound != columnIndex, @"Column %@ could not be found.", columnName);
    
    return [self numberForColumnIndex: columnIndex];
}

- (NSNumber*) numberForColumnIndex: (NSUInteger) columnIndex
{
    if(nil == _internalMySQLRow[columnIndex])
    {
        return nil;
    } // End of entry is nil
    
    NSString * temp = [NSString stringWithUTF8String: _internalMySQLRow[columnIndex]];
    
    NSNumberFormatter * formatter = [[NSNumberFormatter alloc] init];
    [formatter setNumberStyle:NSNumberFormatterDecimalStyle];
    
    return [formatter numberFromString: temp];
} // End of numberForColumnIndex:


@end
