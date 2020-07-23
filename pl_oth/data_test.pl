#!/usr/bin/perl
# data_test.pl
#
# for selected tables generate records:
# TAB_NAME;COL_NAME;DATA_TYPE;MAN;CNT;CNT_NULL;CNT_DISTINCT;UNIQUE;MIN;MAX

use strict ;
use warnings ;
use integer ;
use DBI ;

require 'ora_connect.pl' ;

# -- BEGIN --

$ENV{"NLS_SORT"} = 'BINARY' ;
unless( $ARGV[ 0 ] ) { die "No args (connect string expected).\n\n" }

my $Lda = ora_connect( $ARGV[ 0 ] ) ;

if( $Lda )
  {
   alter_session() ;
   #my $c_tabs = $Lda->prepare("select TABLE_NAME from USER_TABLES where TABLE_NAME like '%ADDRESS' order by 1") ;
   my $c_tabs = $Lda->prepare("select TABLE_NAME from USER_TABLES WHERE TABLE_NAME like 'WDM_CITI_LDWH_H_SALES' order by 1") ;
   my $table_name ;

   print 'TAB_NAME;COL_NAME;DATA_TYPE;MAN;CNT;CNT_NULL;CNT_DISTINCT;UNIQUE;MIN;MAX'."\n" ;

   $c_tabs->execute() ;
   while( ( $table_name ) = $c_tabs->fetchrow_array() )
     {
      table_test( $table_name ) ;
     }

   $Lda->disconnect() or warn "Disconnect: $DBI::errstr\n\n" ;
  }

# -- END --

sub alter_session
  {
   my $c_set = $Lda->prepare("alter session set NLS_DATE_FORMAT='YYYY-MM-DD HH24:MI:SS'") ;
   $c_set->execute() ;
  }


sub get_ra_Tcols
  {
   my ( $table_name ) = @_ ;
   my ( $col_name, $data_type, $mandatory ) ;
   my @Tcols ;

   my $c_col = $Lda->prepare("
SELECT
  A.column_name,
  CASE
    WHEN A.data_type = 'NUMBER'
    THEN
      CASE
        WHEN A.data_precision IS NULL
        THEN A.data_type
        ELSE 
          CASE
            WHEN A.data_scale = 0
            THEN A.data_type||'('||TO_CHAR( A.data_precision )||')'
            ELSE A.data_type||'('||TO_CHAR( A.data_precision )||','||TO_CHAR( A.data_scale )||')'
          END
      END
    WHEN A.data_type = 'CHAR'
    THEN
      CASE
        WHEN A.data_length = 1
        THEN A.data_type
        ELSE A.data_type||'('||TO_CHAR( A.data_length )||')'
      END
    WHEN A.data_type LIKE 'VARCHAR%'
    THEN A.data_type||'('||TO_CHAR( A.data_length )||')'
    ELSE A.data_type
  END AS data_type,
  case when A.nullable = 'N' then 'Y' end as MAN
FROM
  user_tab_columns A
WHERE
  A.table_name = ?
ORDER BY
  A.column_id") ;

   $c_col->execute( $table_name ) ;
   while( ( $col_name, $data_type, $mandatory ) = $c_col->fetchrow_array())
     {
      unless( $mandatory ) { $mandatory = ''}
      push( @Tcols, [ $col_name, $data_type, $mandatory ] ) ;
     }

   return \@Tcols ;
  }


sub get_sql1
  {
   my ( $table_name, $ra_Tcols ) = @_ ;
   my ( $ra_Col, $sql, $col_name, $pref ) ;
   my $ix = 0 ;

   $sql = 'select'."\n".
          '  count(*) as CNT_ALL' ;

   foreach $ra_Col ( @$ra_Tcols )
     {
      $col_name = $ra_Col->[0] ;
      ++$ix ;
      $pref = 'C'. $ix .'_' ;
      $sql .= ",\n".
              '  count('. $col_name .') as '. $pref .'CA,'."\n".
              '  count(distinct '. $col_name .') as '. $pref .'CD,'."\n".
              '  min('. $col_name .') as '. $pref .'MI,'."\n".
              '  max('. $col_name .') as '. $pref .'MA' ;
     }

   $sql .= "\n".
           'from'."\n".
           '  '. $table_name ;

   return $sql ;
  }


sub run_sql1
  {
   my ( $sql, $col_cnt ) = @_ ;
   my ( @Val, $cnt_all ) ;
   my ( $ca,   # count
        $cd,   # count distinct
        $cn,   # count(NULL)
        $un,   # unique (Y/N)
        $min,
        $max ) ;
   my ( $ix, $iv ) ;
   my @T_va1 ;
   my $c1 = $Lda->prepare( $sql ) ;

   $c1->execute() ;
   @Val = $c1->fetchrow_array() ;
   $c1->finish() ;

   $cnt_all = $Val[0] ; # -- count(*)

   $iv = 1 ;
   for( $ix = 0 ; $ix < $col_cnt ; ++$ix )
     {
      ( $ca, $cd, $min, $max ) = ( $Val[ $iv ], $Val[ $iv+1 ], $Val[ $iv+2 ], $Val[ $iv+3 ] ) ;
      $iv += 4 ;

      $cn = $cnt_all - $ca ;
      $un = (($cnt_all - $cn) == $cd && $cd > 0 ) ? 'Y':'' ;
      if( $cd == 1 ) { $max = '' }

      if( defined( $min ) )
        {
         if( length( $min ) > 30 ) { $min = substr( $min, 0, 25 ) .'...' }
        }
      else
        { $min = '' }

      if( defined( $max ) )
        {
         if( length( $max ) > 30 ) { $max = substr( $max, 0, 25 ) .'...' }
        }
      else
        { $max = '' }

      push( @T_va1, [ $ca, $cn, $cd, $un, $min, $max ] ) ;
     }

   return \@T_va1 ;
  }


sub table_test
  {
   my ( $table_name ) = @_ ;
   my ( $ra_Tcols, $ra_Col ) ;
   my ( $ra_Tva1,  $ra_Va1 ) ;
   my ( $sql, $col_cnt ) ;
   my $ix ;

   $ra_Tcols = get_ra_Tcols( $table_name ) ;
   $col_cnt = @$ra_Tcols ; # -- count of columns

=pod
   foreach $ra_Col ( @$ra_Tcols )
     {
      print sprintf("%-30s", $ra_Col->[0] ) .' '.
            sprintf("%-15s", $ra_Col->[1] ) .' '.
            $ra_Col->[2] ."\n" ;
     }
=cut

   $sql = get_sql1( $table_name, $ra_Tcols ) ;
   $ra_Tva1 = run_sql1( $sql, $col_cnt ) ;

   for( $ix = 0 ; $ix < $col_cnt ; ++$ix )
     {
      $ra_Col = $ra_Tcols->[ $ix ] ;
      $ra_Va1 = $ra_Tva1->[ $ix ] ;

      print join(";", ( $table_name,
                        $ra_Col->[ 0 ], # -- column name
                        $ra_Col->[ 1 ], # -- data type
                        $ra_Col->[ 2 ], # -- mandatory
                        $ra_Va1->[ 0 ], # -- count
                        $ra_Va1->[ 1 ], # -- count(NULL)
                        $ra_Va1->[ 2 ], # -- count distinct
                        $ra_Va1->[ 3 ], # -- unique
                        $ra_Va1->[ 4 ], # -- min
                        $ra_Va1->[ 5 ]  # -- max
                        )) ."\n" ;
     }
  }

