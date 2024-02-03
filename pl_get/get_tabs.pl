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
#
# 2024-02-02 (last update)

use strict ;
use warnings ;
use integer ;
use DBI ;
use FileHandle ;
use Ora_LDA ;

# -- BEGIN --

$ENV{"NLS_SORT"} = 'BINARY' ;
my ( $Userid, $ra_Names ) = get_input_params( \@ARGV ) ;
my $Lda ;

# connect to Oracle DB
if( $Lda = Ora_LDA::ora_LDA( $Userid ))
  {
   save_label('00_db_tabs.lst') ;

   # print 'LongReadLen: '. $Lda->{LongReadLen} ."\n" ;
   $Lda->{LongReadLen} = 4096 ;

   my $table_name ;
   my $c_tabs = $Lda->prepare( q{
select a.TABLE_NAME
from
  USER_TABLES a
where
  regexp_like( a.TABLE_NAME, ?,'i')
order by
  a.TABLE_NAME
} ) ;

   for my $tab_name_RE ( @$ra_Names )
     {
      $c_tabs->execute( $tab_name_RE ) ;
      while( ( $table_name ) = $c_tabs->fetchrow_array() )
        {
         upload_table( $table_name ) ;
        }
     }

   $Lda->disconnect() or warn "Disconnect: $DBI::errstr\n\n" ;
  }

# -- END --

sub get_input_params
  {
   my ( $ra_arg ) = @_ ;
   my ( $userid, # Oracle connect string
        @a_name  # table name regexp values
      ) ;

   @a_name = () ;
   foreach my $val ( @$ra_arg )
     {
      if( $val =~ /^[\w]+\/[\S]+@[\w\.]+$/
          || ( $val =~ /^\w+$/ && exists $ENV{ $val } ) )
        {
         if( ! defined( $userid )) { $userid = $val }
         else
           { print STDERR 'Unexpected value (userid): '. $val ."\n" }
        }
      else
        { push( @a_name, $val ) }
     }

   if( scalar @a_name == 0 )  { push( @a_name,'.*') }

   unless(    defined( $userid )
           && scalar @a_name != 0 )
     {
      if( scalar @$ra_arg > 0 ) { print STDERR 'Invalid parameters'."\n" }
      printf STDERR 'Usage: '. basename( $0 ) .' <userid> [<table_name_RE>] ...'."\n\n" ;
      exit 1 ;
     }

   return ( $userid, \@a_name ) ;
  }


sub save_label
  {
   my ( $fname ) = @_ ;
   my ( $sysdate, $uid, $out ) ;

   $sysdate = Ora_LDA::get_sysdate() ;
   $uid     = Ora_LDA::get_uid( $Lda ) ;

   open( $out,'>', $fname ) || die 'Error on open('. $fname .'): '. $! ."\n\n" ;
   print $out 'Date:   '. $sysdate ."\n".
              'Schema: '. $uid ."\n" ;
   close( $out ) ;

   print 'File '. $fname .' created.'."\n" ;
  }


sub print_pk_for_iot
  {
   my ( $fh, $table_name ) = @_ ;
   my ( $con_name, $col_name, $col_pos ) ;

   my $c_pk = $Lda->prepare( q{
select
  lower( a.CONSTRAINT_NAME ),
  lower( b.COLUMN_NAME ),
  b.COLUMN_POSITION
from
  USER_CONSTRAINTS a
  inner join USER_IND_COLUMNS b on ( a.CONSTRAINT_NAME = b.INDEX_NAME )
where
  a.TABLE_NAME = ?
  and a.CONSTRAINT_TYPE = 'P'
order by
  b.COLUMN_POSITION
} ) ;

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

   my $c_pa = $Lda->prepare( q{
select a.PARTITION_NAME, a.HIGH_VALUE
from   USER_TAB_PARTITIONS a
where  a.TABLE_NAME = ?
order by a.PARTITION_POSITION
} ) ;

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

   my $c_pt = $Lda->prepare( q{
select
  a.PARTITIONING_TYPE,
  a.SUBPARTITIONING_TYPE,
  a.PARTITION_COUNT,
  lower( b.COLUMN_NAME )
from
  USER_PART_TABLES a
  inner join USER_PART_KEY_COLUMNS b on ( a.TABLE_NAME = b.NAME
                                          and 'TABLE'  = b.OBJECT_TYPE )
where
  a.TABLE_NAME = ?
order by
  b.COLUMN_POSITION
} ) ;

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
   my $c_col = $Lda->prepare( q{
select lower(a.COLUMN_NAME), a.DATA_TYPE, a.DATA_LENGTH, a.DATA_PRECISION, a.DATA_SCALE,
       a.DATA_DEFAULT, a.NULLABLE
from   USER_TAB_COLUMNS a
where  a.TABLE_NAME = ?
order by a.COLUMN_ID
} ) ;

 # print $fh 'PROMPT Table '. $table_name ."\n\n" ;

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
   my $c_con = $Lda->prepare( q{
select a.CONSTRAINT_NAME, a.SEARCH_CONDITION
from   USER_CONSTRAINTS a
where  a.CONSTRAINT_NAME not like 'SYS%'
       and a.CONSTRAINT_TYPE = 'C'
       and a.TABLE_NAME = ?
order by a.CONSTRAINT_NAME
} ) ;

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

   $c_tc1 = $Lda->prepare( q{
select a.TABLE_TYPE, a.COMMENTS
from   USER_TAB_COMMENTS a
where  a.TABLE_NAME = ?
       and a.COMMENTS is not null
} ) ;

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

   $c_tc2 = $Lda->prepare( q{
select a.COLUMN_NAME, a.COMMENTS
from   USER_COL_COMMENTS a
where  a.TABLE_NAME = ?
       and a.COMMENTS is not null
order by a.COLUMN_NAME
} ) ;

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
   my $c_tab = $Lda->prepare( q{
select
  a.TABLESPACE_NAME,
  a.CLUSTER_NAME,
  decode( nvl( a.LOGGING,'YES'),'YES','Y','N') as LOGGING,
  decode( a.PARTITIONED,'YES','Y','N')         as PARTITIONED,
  decode( nvl( a.IOT_TYPE,'-'),'IOT','Y','N')  as IOT,
  a.TEMPORARY,
  a.DURATION,
  ( select count( b.INDEX_NAME )
    from   USER_INDEXES b
    where  b.TABLE_NAME = a.TABLE_NAME )       as INDEXES,
  ( select count( c.TRIGGER_NAME )
    from   USER_TRIGGERS c
    where  c.TABLE_NAME = a.TABLE_NAME )       as TRGS
from
  USER_TABLES a
where
  a.TABLE_NAME = ?
} ) ;

   $c_tab->execute( $table_name ) ;
   ( $tabsp_name, $clust_name, $is_log, $is_part, $is_iot, $is_temp, $duration,
     $cnt_i, $cnt_t ) = $c_tab->fetchrow_array() ;
   $c_tab->finish() ;

   if( $c_tab->rows > 0 )
     {
      $fh = new FileHandle ;

      $fname = lc( $table_name ) .'_TA.sql' ;
      $fh->open('>'. $fname ) ;
      print $fh '-- '. $fname ."\n".
                '--'."\n".
                '-- rdbms: oracle'."\n\n" ;

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
   my ( $fname, $out ) ;
   my $c_ind = $Lda->prepare( q{
select
  row_number() over (order by a.INDEX_NAME, b.COLUMN_POSITION )             as IND_R,
  row_number() over (partition by a.INDEX_NAME order by b.COLUMN_POSITION ) as IND_C,
  count( b.COLUMN_POSITION ) over (partition by a.INDEX_NAME )              as SUM_C,
  a.INDEX_NAME,
  a.INDEX_TYPE,
  a.UNIQUENESS,
  decode( a.PARTITIONED,'yes', d.LOCALITY ) as LOCALITY,
  c.CONSTRAINT_TYPE as CTYPE,
  b.COLUMN_NAME
from
  USER_INDEXES a
  inner join USER_IND_COLUMNS b       on ( a.INDEX_NAME = b.INDEX_NAME )
  left outer join USER_CONSTRAINTS c  on ( a.INDEX_NAME = c.CONSTRAINT_NAME )
  left outer join USER_PART_INDEXES d on ( a.INDEX_NAME = d.INDEX_NAME )
where
  a.TABLE_NAME = ?
  and ('Y' != ? or nvl( c.CONSTRAINT_TYPE,'-') != 'P') -- leaves out PK for index organized table
order by
  a.INDEX_NAME,
  b.COLUMN_POSITION
} ) ;

   $c_ind->execute( $table_name, $is_iot ) ;

   while( ( $ind_r, $ind_c, $sum_c, $ind_name, $ind_type, $uniq, $locality, $ctype, $col_name )
          = $c_ind->fetchrow_array() )
     {
      if( $ind_r == 1 )
        {
         $table_name2 = lc( $table_name ) ;
         $fname = $table_name2 .'_IN.sql' ;
         open( $out,'>', $fname ) || die 'Error on open('. $fname .'): '. $! ."\n\n" ;
         print $out '-- '. $fname ."\n".
                   '--'."\n".
                   '-- rdbms: oracle'."\n" ;

       # print $out 'PROMPT Indexes on table '. $table_name ."\n" ;
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
            print $out "\n".
                       'ALTER TABLE '. $table_name2 .' ADD'."\n".
                       'CONSTRAINT '. $ind_name2 .' '. $ctype2 .' ( '. $col_names .' )' ;
           }
         else
           {
            if( $ind_type2 ne 'BITMAP')  { $ind_option = '' }
            else
              { $ind_option = $ind_type2 .' ' }

            print $out "\n".
                       'CREATE '. $ind_option .'INDEX '. $ind_name2 .' ON '.
                       $table_name2 .' ( '. $col_names .' )' ;
           }

         if( $ctype2 )
           {
            if( defined $locality2 && $locality2 eq 'LOCAL' )
              {
               print $out "\n".'USING INDEX LOCAL'.
                          "\n".'TABLESPACE &TABSP_NAME' ;
              }
            else
              {
               print $out "\n".'USING INDEX'.
                          "\n".'TABLESPACE &TABSP_NAME' ;
              }
           }
         else
           {
            if( defined $locality2 && $locality2 eq 'LOCAL' )
              {
               print $out "\n".'LOCAL'.
                          "\n".'TABLESPACE &TABSP_NAME'.
                          "\n".'PARALLEL' ;
              }
            else
              { print $out "\n".'TABLESPACE &TABSP_NAME' }
           }

         print $out "\n/\n" ;
        }
     }

   if( $c_ind->rows > 0 )
     {
      close( $out ) ;
      print 'File '. $fname .' created.'."\n" ;
     }
  }


sub upload_tab_triggers
  {
   my ( $table_name, $table_type ) = @_ ;
   my ( $trg_name, $row_no ) ;
   my ( $fname, $out ) ;
   my $text ;

   my $c_trg = $Lda->prepare( q{
select
  a.TRIGGER_NAME,
  row_number() over (order by a.TRIGGER_NAME ) as ROW_NO
from
  USER_TRIGGERS a
where
  a.TABLE_NAME = ?
  and a.BASE_OBJECT_TYPE = ?
order by
  a.TRIGGER_NAME
} ) ;

   my $c_src = $Lda->prepare( q{
select rtrim( a.TEXT ) from USER_SOURCE a where a.NAME = ? and a.TYPE = ?
order by a.LINE
} ) ;

   $c_trg->execute( $table_name, $table_type ) ;

   while( ( $trg_name, $row_no ) = $c_trg->fetchrow_array() )
     {
      if( $row_no == 1 )
        {
         $fname = lc( $table_name ) .'_TR.sql' ;
         open( $out,'>', $fname ) || die 'Error on open('. $fname .'): '. $! ."\n\n" ;
         print $out '-- '. $fname ."\n".
                    '--'."\n".
                    '-- rdbms: oracle'."\n" ;
        }

    # print $out 'PROMPT Trigger '. uc( $trg_name ) ."\n\n" ;

      print $out 'CREATE OR REPLACE ' ;
      $c_src->execute( $trg_name,'TRIGGER') ;
      while( ( $text ) = $c_src->fetchrow_array() ) { print $out $text }

      print $out "/\n\n" ;
     }

   if( $c_trg->rows > 0 )
     {
      close( $out ) ;
      print 'File '. $fname .' created.'."\n" ;
     }
  }

# --- End of get_tabs.pl
