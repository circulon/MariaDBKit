//
//  MariaDBResultSet.h
//  MariaDBKit
//
//  Created by Kyle Hankinson on 2019-03-06.
//  Copyright © 2019 Kyle Hankinson. All rights reserved.
//

#import <Foundation/Foundation.h>

NS_ASSUME_NONNULL_BEGIN

@interface MariaDBResultSet : NSObject


- (id) objectForColumnIndex: (NSUInteger) columnIndex;
- (id) objectForColumn: (NSString*) columnName;
- (BOOL) columnIsNull:(NSString*)columnName;
- (BOOL) columnAtIndexIsNull:(NSInteger)columnIndex;

- (BOOL) nextRow;
- (NSDictionary*) rowAsDictionary;
- (NSArray *) allRows;
- (void)reset;

@property(nonatomic,readonly) NSUInteger rowCount;
@property(nonatomic,retain,readonly) NSArray<NSString*>* columnNames;

@end

NS_ASSUME_NONNULL_END
