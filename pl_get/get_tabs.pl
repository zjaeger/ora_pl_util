#!/usr/bin/perl
# get_tabs.pl
#
# SCHEMA UPLOAD (into text-files):
# - selected tables (+ check constraints, comments)
# - indexes (for selected tables only)
# - triggers (for selected tables only)
#
# USAGE: get_tabs.pl <oracle_connect_string>
# (oracle_connect_string: username/password@db_name)
# TODO:
# - check constraints: remove empty rows
# - triggers: problem with "end ;/"

use strict ;
use warnings ;
use integer ;
use DBI ;
use FileHandle ;
use Ora_LDA ;

# -- BEGIN --

$ENV{"NLS_SORT"} = 'BINARY' ;

unless( $ARGV[ 0 ] ) { die "No args (connect string expected).\n\n" }

my $Lda = Ora_LDA::ora_LDA( $ARGV[ 0 ] ) ;

if( $Lda )
  {
   save_label() ;

   # print 'LongReadLen: '. $Lda->{LongReadLen} ."\n" ;
   $Lda->{LongReadLen} = 4096 ;

#  upload_table('DEBITS') ;
#  upload_table('ACCOUNTS') ;
#  upload_table('TRANSACTIONS') ;
#  upload_table('PROJ_CONTRACTS') ;
#  upload_table('TEMP_DEBITS') ;
#  upload_table('DEBITS_MONTHLY_HISTORY') ;

   my $table_name ;
   my $c_tabs = $Lda->prepare("\
SELECT table_name FROM user_tables") ;

   $c_tabs->execute() ;
   while( ( $table_name ) = $c_tabs->fetchrow_array() )
     {
      upload_table( $table_name ) ;
     }

   $Lda->disconnect() or warn "Disconnect: $DBI::errstr\n\n" ;
  }

# -- END --

sub save_label
  {
   my ( $sysdate, $uid, $fname ) ;

   $sysdate = Ora_LDA::get_sysdate() ;
   $uid     = Ora_LDA::get_uid( $Lda ) ;

   $fname = '00_db_tabs.lst' ;

   open( OUT, ">$fname") || die "Can't open file ". $fname ."\n\n" ;
   print OUT 'Date:   '. $sysdate ."\n".
             'Schema: '. $uid ."\n" ;
   close( OUT ) ;

   print 'File '. $fname .' created.'."\n" ;
  }


sub print_pk_for_iot
  {
   my ( $fh, $table_name ) = @_ ;
   my ( $con_name, $col_name, $col_pos ) ;

   my $c_pk = $Lda->prepare("\
SELECT
  LOWER( A.constraint_name ),
  LOWER( B.column_name ),
  B.column_position
FROM
  user_constraints A
  INNER JOIN user_ind_columns B ON ( A.constraint_name = B.index_name )
WHERE
  A.table_name = ?
  AND A.constraint_type = 'P'
ORDER BY
  B.column_position") ;

   $c_pk->execute( $table_name ) ;
   while( ( $con_name, $col_name, $col_pos ) = $c_pk->fetchrow_array() )
     {
      if( $col_pos == 1 )
        {
         print $fh ",\n".'CONSTRAINT '. $con_name .' PRIMARY KEY('."\n".
                   '  '. $col_name ;
        }
      else
        { print $fh ",\n  ". $col_name }
     }
   print $fh " )\n".")\n".'ORGANIZATION INDEX' ;
  }


sub print_partitions
  {
   my ( $fh, $table_name, $part_type ) = @_ ;
   my ( $part_name, $val ) ;
   my $keywords ;

   my $c_pa = $Lda->prepare("\
SELECT partition_name, high_value
FROM   user_tab_partitions
WHERE  table_name = ?
ORDER BY partition_position") ;

   if(    $part_type eq 'LIST' )  { $keywords = 'VALUES' }
   elsif( $part_type eq 'RANGE')  { $keywords = 'VALUES LESS THAN' }
   elsif( $part_type eq 'HASH' )  { $keywords = '' }
   else                           { $keywords = '/* ERROR (part_type='. $part_type .') */' }

   $c_pa->execute( $table_name ) ;

   while( ( $part_name, $val ) = $c_pa->fetchrow_array() )
     {
      if( $c_pa->rows > 1 ) { print $fh ',' }

      if( $keywords )
        { print $fh "\n".'  PARTITION '. $part_name .' '. $keywords .' ('. $val .')' }
      else
        { print $fh "\n".'  PARTITION '. $part_name }  # -- for hash partitions
     }
  }


sub print_partition_clause
  {
   my ( $fh, $table_name ) = @_ ;
   my ( $part_type, $subpart_type, $part_cnt, $col_name ) ;
   my $part_type0 ;

   my $c_pt = $Lda->prepare("\
SELECT
  A.partitioning_type,
  A.subpartitioning_type,
  A.partition_count,
  LOWER(B.column_name)
FROM
  user_part_tables A
  INNER JOIN user_part_key_columns B
    ON ( A.table_name = B.name
         AND 'TABLE' = B.object_type )
WHERE
  A.table_name = ?
ORDER BY
  B.column_position") ;

   $c_pt->execute( $table_name ) ;
   while( ( $part_type, $subpart_type, $part_cnt, $col_name ) = $c_pt->fetchrow_array() )
     {
      if( $c_pt->rows == 1 )
        {
         if( $subpart_type ne 'NONE' )
           {
            print 'Warning: subpartition_type='. $subpart_type .' for table_name='. $table_name ."\n" ;
           }
         print $fh "\n".'PARTITION BY '. $part_type .' ('. $col_name ;
         $part_type0 = $part_type ;
        }
      else
        { print $fh ', '. $col_name }
     }
   print $fh ') (' ;

   print_partitions( $fh, $table_name, $part_type0 ) ;

   print $fh "\n)" ;
  }


sub upload_table_1  # table specification
  {
   my ( $fh, $table_name, $tabsp_name, $clust_name, $is_log, $is_part, $is_iot,
        $is_temp, $duration ) = @_ ;
# neuplne: nebere v potaz nekt.atributy (tablespace, cluster, ...)
   my ( $col_name, $data_type, $data_len, $data_prec, $data_sc, $data_def, $null ) ;
   my $dtype ;
   my $c_col = $Lda->prepare("
SELECT LOWER(column_name), data_type, data_length, data_precision, data_scale,
       data_default, nullable
FROM   user_tab_columns
WHERE  table_name = ?
ORDER BY column_id") ;

   print $fh 'PROMPT Table '. $table_name ."\n\n" ;

   if( $is_temp ne 'Y' )
     { print $fh 'CREATE TABLE ' }
   else
     { print $fh 'CREATE GLOBAL TEMPORARY TABLE ' }

   print $fh lc( $table_name ) ."(" ;

# -- table columns

   $c_col->execute( $table_name ) ;

   while( ( $col_name, $data_type, $data_len, $data_prec, $data_sc,
            $data_def, $null ) = $c_col->fetchrow_array() )
     {
      if( $data_type eq 'NUMBER' )
        {
         if( $data_prec )
           {
            if( $data_sc == 0 )
              { $dtype = $data_type .'('. $data_prec .')' }
                else
              { $dtype = $data_type .'('. $data_prec .','. $data_sc .')' }
           }
         else
           { $dtype = $data_type }
# ----------------------- specialita pro DD: start ---------------------
#          {
#           unless( $col_name =~ /_id$/ )
#             { $dtype = $data_type ; }
#           else
#             { $dtype = $data_type .'(12)' }
#          }
# ----------------------- specialita pro DD: konec ---------------------
        }
      elsif ( $data_type =~ /CHAR/ )
        { $dtype = $data_type .'('. $data_len .')' }
      else
        { $dtype = $data_type }

      if( $c_col->rows > 1 )
        { printf $fh ",\n  %-30s  %-20s", $col_name, $dtype }
      else
        { printf $fh "\n  %-30s  %-20s", $col_name, $dtype }

      if( defined $data_def )
        {
         $data_def =~ s/[\s]+$// ;
         if( ( $data_def && uc($data_def) ne 'NULL') || $data_def eq '0' )
           {
            print $fh ' DEFAULT '. $data_def ;
## -- @@ spec: begin
##       if( $data_def eq '0' )
##         { print 'ALTER TABLE '. lc( $table_name ) .' MODIFY ( '. $col_name .' DEFAULT 0 ) ;'."\n" }
## -- @@ spec: end
           }
        }

      if( $null eq 'Y' )
        { print $fh ' NULL' }
      else
        { print $fh ' NOT NULL' }
     }

# -- table options

   if( $is_iot ne 'Y' )
     { print $fh "\n)" }
   else
     { print_pk_for_iot( $fh, $table_name ) }  # -- for index organised table

   if( $is_part eq 'Y' )
     {
      print_partition_clause( $fh, $table_name ) ;
     }

   if( $is_temp eq 'Y' )  # -- temporary table
     {
      if( $duration =~ /SESSION$/ )
        { print $fh "\n".'ON COMMIT PRESERVE ROWS' }
      else
        { print $fh "\n".'ON COMMIT DELETE ROWS' }
     }

   if( $is_log ne 'Y' && $is_temp ne 'Y')  { print $fh "\n".'NOLOGGING' }

   print $fh "\n".'TABLESPACE &TABSP_NAME'."\n".'/'."\n\n" ;

   ## print $fh " ;\n\n" ;
  }


sub upload_table_2  # -- check constraints
  {
   my ( $fh, $table_name ) = @_ ;

   my ( $con, $text ) ;
   my $c_con = $Lda->prepare("\
SELECT constraint_name, search_condition
FROM   user_constraints
WHERE  constraint_name NOT LIKE 'SYS%'
       AND constraint_type = 'C'
       AND table_name = ?
ORDER BY constraint_name") ;

   $c_con->execute( $table_name ) ;

   while( ( $con, $text ) = $c_con->fetchrow_array() )
     {
      $text =~ s/[\t ]+/ /g ;
      print $fh 'ALTER TABLE '. lc( $table_name ) .' ADD'."\n".
                'CONSTRAINT '. lc( $con ) .' CHECK ('."\n".
                ' '. $text .' )'."\n".
                '/'."\n\n" ;
     }
  }


sub upload_table_3  # -- table and column comments
  {
   my ( $fh, $table_name ) = @_ ;
   my ( $c_tc1, $c_tc2 ) ;
   my ( $type, $col_name, $text ) ;

   $c_tc1 = $Lda->prepare("
SELECT table_type, comments
FROM   user_tab_comments
WHERE  table_name = ?
       AND comments IS NOT NULL") ;

   $c_tc1->execute( $table_name ) ;
   ( $type, $text ) = $c_tc1->fetchrow_array() ;
   $c_tc1->finish() ;

   if( $type )
     {
      $text =~ s/\'/\'\'/g ;
      print $fh 'COMMENT ON '. $type .' '. $table_name .' IS'."\n".
                "\'". $text ."\'"."\n".
                "/\n" ;
     }

   $c_tc2 = $Lda->prepare("
SELECT column_name, comments
FROM   user_col_comments
WHERE  table_name = ?
       AND comments IS NOT NULL
ORDER BY column_name") ;

   $c_tc2->execute( $table_name ) ;
   while( ( $col_name, $text ) = $c_tc2->fetchrow_array() )
     {
      $text =~ s/\'/\'\'/g ;
      print $fh 'COMMENT ON COLUMN '. $table_name .'.'. $col_name .' IS'."\n".
                "\'". $text ."\'"."\n".
                "/\n" ;
     }
  }


sub upload_table
  {
   my ( $table_name ) = @_ ;
   my ( $fname, $fh ) ;
   my ( $tabsp_name,  # not used
        $clust_name,  # not used (warning: if not null)
        $is_log,
        $is_part,     # partioned table (Y/N)
        $is_iot,      # index organized table (Y/N)
        $is_temp,     # temporary table (Y/N)
        $duration,    # for temp.table; values: 'SYS$SESSION','SYS$TRANSACTION'
        $cnt_i, $cnt_t ) ;
   my $c_tab = $Lda->prepare("\
SELECT
  A.tablespace_name,
  A.cluster_name,
  DECODE( NVL( A.logging,'YES'),'YES','Y','N') AS logging,
  DECODE( A.partitioned,'YES','Y','N')         AS partitioned,
  DECODE( NVL( A.iot_type,'x'),'IOT','Y','N')  AS iot,
  A.temporary,
  A.duration,
  ( SELECT COUNT( B.index_name )
    FROM   user_indexes B
    WHERE  B.table_name = A.table_name )       AS indexes,
  ( SELECT COUNT( C.trigger_name ) 
    FROM   user_triggers C
    WHERE  C.table_name = A.table_name )       AS trgs
FROM
  user_tables A
WHERE
  A.table_name = ?") ;

   $c_tab->execute( $table_name ) ;
   ( $tabsp_name, $clust_name, $is_log, $is_part, $is_iot, $is_temp, $duration,
     $cnt_i, $cnt_t ) = $c_tab->fetchrow_array() ;
   $c_tab->finish() ;

   if( $c_tab->rows > 0 )
     {
      $fh = new FileHandle ;

      $fname = lc( $table_name ) .'_TA.sql' ;
      $fh->open('>'. $fname ) ;
      print $fh '-- '. $fname ."\n\n" ;

      # -- table specification 

      if( $clust_name )
        {
         print 'Warning: cluster '. $clust_name .' ignored (table name: '. $table_name .")\n" ;
        }

      upload_table_1( $fh, $table_name, $tabsp_name, $clust_name,
                      $is_log, $is_part, $is_iot, $is_temp, $duration ) ;

      # -- check constraints

      upload_table_2( $fh, $table_name ) ;

      # -- comments

      upload_table_3( $fh, $table_name ) ;

      $fh->close() ;
      print 'File '. $fname .' created.'."\n" ;

      # -- indexes (into other file)

      if( $cnt_i > 0 )  { upload_tab_indexes( $table_name, $is_iot ) }

      # -- triggers (into other file)

      if( $cnt_t > 0 )  { upload_tab_triggers( $table_name,'TABLE') }
     }
   else
     {
      print 'Error: uknown table name: '. $table_name ."\n" ;
     }
  }


sub upload_tab_indexes
  {
   # $is_iot = 'Y' (index organized table) => vynechava se "primary constraint index"

   my ( $table_name, $is_iot ) = @_ ;

   my ( $ind_r,  # -- poradove cislo
        $ind_c,  # -- poradove cislo v ramci jednoho indexu
        $sum_c,  # -- pocet sloupcu pro index
        $ind_name, $ind_type, $uniq, $locality, $ctype, $col_name ) ;
   my ( $table_name2, $ind_name2, $ind_type2, $locality2, $ctype2, $col_names ) ;
   my $ind_option ;
   my $fname ;
   my $c_ind = $Lda->prepare("\
SELECT
  ROW_NUMBER() OVER (ORDER BY A.index_name, B.column_position )             AS ind_r,
  ROW_NUMBER() OVER (PARTITION BY A.index_name ORDER BY B.column_position ) AS ind_c,
  COUNT( B.column_position ) OVER (PARTITION BY A.index_name )              AS sum_c,
  A.index_name,
  A.index_type,
  A.uniqueness,
  DECODE( A.partitioned,'YES', D.locality ) AS locality,
  C.constraint_type AS ctype,
  B.column_name
FROM
  user_indexes A
  INNER JOIN user_ind_columns B       ON ( A.index_name = B.index_name )
  LEFT OUTER JOIN user_constraints C  ON ( A.index_name = C.constraint_name )
  LEFT OUTER JOIN user_part_indexes D ON ( A.index_name = D.index_name )
WHERE
  A.table_name = ?
  AND ('Y' != ? OR NVL( C.constraint_type,'X') != 'P') -- leaves out PK for index organized table
ORDER BY
  A.index_name,
  B.column_position") ;

   $c_ind->execute( $table_name, $is_iot ) ;

   while( ( $ind_r, $ind_c, $sum_c, $ind_name, $ind_type, $uniq, $locality, $ctype, $col_name )
          = $c_ind->fetchrow_array() )
     {
      if( $ind_r == 1 )
        {
         $table_name2 = lc( $table_name ) ;
         $fname = $table_name2 .'_IN.sql' ;
         open( OUT,'>'. $fname ) or die "Can't open file ". $fname ."\n\n" ;
         print OUT '-- '. $fname ."\n\n" ;

         print OUT 'PROMPT Indexes on table '. $table_name ."\n" ;
        }

      if( $ind_c == 1 )  # the first row for the table index
        {
         $ind_name2 = lc( $ind_name ) ;
         $ind_type2 = $ind_type ;
         $locality2 = $locality ;
         $col_names = lc( $col_name ) ;

         if( $locality2 )  # -- not null only for partitioned index
           {
            if( $locality2 ne 'LOCAL' )
              {
               print 'Warning: partitioned index '. $ind_name .
                     ' with unsuported locality: '. $locality2 ."\n" ;
               $locality2 = '' ;
              }
           }

         unless( $ctype )
           {
# pozn.: pro UNIQUE index, pro kt. neexistuje UNIQUE CONSTRAINT,
# se tento constraint generuje
            if( $uniq ne 'UNIQUE' )  { $ctype2 = '' }
            else
              {
               $ctype2 = $uniq ;
               print 'Warning: UNIQUE index '. $ind_name .' on '. $table_name .
                     ' without according unique constraint exists'."\n" ;
              }
           }
         else
           {
            if( $ctype eq 'P' ) { $ctype2 = 'PRIMARY KEY' } else { $ctype2 = 'UNIQUE' }
           }
        }
      else
        { $col_names .= ', '. lc( $col_name ) }

      if( $ind_c == $sum_c )  # the last row for the table index
        {
         if( $ctype2 )
           { 
            print OUT "\n".
                      'ALTER TABLE '. $table_name2 .' ADD'."\n".
                      'CONSTRAINT '. $ind_name2 .' '. $ctype2 .' ( '. $col_names .' )' ;
           }
         else
           {
            if( $ind_type2 ne 'BITMAP')  { $ind_option = '' }
            else
              { $ind_option = $ind_type2 .' ' }

            print OUT "\n".
                      'CREATE '. $ind_option .'INDEX '. $ind_name2 .' ON '.
                      $table_name2 .' ( '. $col_names .' )' ;
           }

         if( $ctype2 )
           {
            if( defined $locality2 && $locality2 eq 'LOCAL' )
              {
               print OUT "\n".'USING INDEX LOCAL'.
                         "\n".'TABLESPACE &TABSP_NAME' ;
              }
            else
              {
               print OUT "\n".'USING INDEX'.
                         "\n".'TABLESPACE &TABSP_NAME' ;
              }
           }
         else 
           {
            if( defined $locality2 && $locality2 eq 'LOCAL' )
              {
               print OUT "\n".'LOCAL'.
                         "\n".'TABLESPACE &TABSP_NAME'.
                         "\n".'PARALLEL' ;
              }
            else
              {
               print OUT "\n".'TABLESPACE &TABSP_NAME' ;
              }
           }

         print OUT "\n/\n" ;
        }
     }

   if( $c_ind->rows > 0 )
     {
      close( OUT ) ;
      print 'File '. $fname .' created.'."\n" ;
     }
  }


sub upload_tab_triggers
  {
   my ( $table_name, $table_type ) = @_ ;
   my ( $trg_name, $row_no ) ;
   my $fname ;
   my $text ;

   my $c_trg = $Lda->prepare("\
SELECT
  trigger_name,
  ROW_NUMBER() OVER (ORDER BY trigger_name )  AS row_no
FROM
  user_triggers
WHERE
  table_name = ?
  AND base_object_type = ?
ORDER BY
  trigger_name") ;

   my $c_src = $Lda->prepare("\
SELECT RTRIM( text ) FROM user_source WHERE name = ? AND type = ?
ORDER BY line") ;
  
   $c_trg->execute( $table_name, $table_type ) ;

   while( ( $trg_name, $row_no ) = $c_trg->fetchrow_array() )
     {
      if( $row_no == 1 )
        {
         $fname = lc( $table_name ) .'_TR.pls' ;
         open( OUT, '>'. $fname ) or die "Can't open file: ". $fname ."\n\n" ;
         print OUT '-- '. $fname ."\n\n" ;
        }

      print OUT 'PROMPT Trigger '. uc( $trg_name ) ."\n\n" ;

      print OUT 'CREATE OR REPLACE ' ;
      $c_src->execute( $trg_name,'TRIGGER') ;
      while( ( $text ) = $c_src->fetchrow_array() ) { print OUT $text }

      print OUT "/\n\n" ;
     }

   if( $c_trg->rows > 0 )
     {
      close( OUT ) ;
      print 'File '. $fname .' created.'."\n" ;
     }
  }

# --- End of get_tabs.pl

