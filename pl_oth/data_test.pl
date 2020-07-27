#!/usr/bin/perl
# data_test.pl
#
# for selected tables: generate records (CSV format) to standard output:
# OWNER;TAB_NAME;COL_NAME;DATA_TYPE;MAN;CNT;CNT_NULL;CNT_DISTINCT;UNIQUE;MIN;MAX

use strict ;
use warnings ;
use integer ;
use DBI ;
use Ora_LDA ;

# -- BEGIN --

$ENV{"NLS_SORT"} = 'BINARY' ;
unless( $ARGV[ 0 ] ) { die "No args ( <connect_string> [ [owner.]table_name_LIKE_expr ] ... ).\n\n" }

my $Lda = Ora_LDA::ora_LDA( $ARGV[ 0 ] ) ;

if( $Lda )
  {
   my $c_tabs ;
   my ( $uname,      # current USERNAME
        $arg,        # command line argument
        $owner,      # table owner
        $table_name, # table name
        $rpc,        # row processed count
        $ix ) ;

   alter_session() ;
   $uname  = get_USERNAME() ;
   $c_tabs = $Lda->prepare(
'select TABLE_NAME from ALL_TABLES where OWNER = ? and TABLE_NAME like ? order by TABLE_NAME') ;

   print 'OWNER;TAB_NAME;COL_NAME;DATA_TYPE;MAN;CNT;CNT_NULL;CNT_DISTINCT;UNIQUE;MIN;MAX'."\n" ;

   for( $ix = 1 ; $ix < scalar @ARGV ; ++$ix )
     {
      $rpc = 0 ;
      $arg = $ARGV[ $ix ] ;
      ( $owner, $table_name ) = get_tab_spec( $uname, $arg ) ;

      $c_tabs->execute( $owner, $table_name ) ;
      while( ( $table_name ) = $c_tabs->fetchrow_array() )
        {
         ++$rpc ; table_test( $owner, $table_name ) ;
        }
      if( $rpc == 0 ) { print STDERR 'Warning: '. $arg .' - invalid table name (or like expr.)'."\n" }
     }

   $Lda->disconnect() or warn "Disconnect: $DBI::errstr\n\n" ;
  }

# -- END --

sub get_USERNAME
  {
   my $uname ;
   my $c_usr = $Lda->prepare('select USERNAME from USER_USERS') ;

   $c_usr->execute() ;
   ( $uname ) = $c_usr->fetchrow_array() ;
   $c_usr->finish() ;

   return $uname ;
  }


sub alter_session
  {
   my $c_set = $Lda->prepare("alter session set NLS_DATE_FORMAT='YYYY-MM-DD HH24:MI:SS'") ;
   $c_set->execute() ;
  }


sub get_tab_spec
  {
   my ( $uname, $arg ) = @_ ;
   my ( $owner, $tab_name ) ;

   if( index( $arg,'.') > 0 )
     {
      ( $owner, $tab_name ) = split(/\./, uc( $arg )) ;
     }
   else
     {
      $owner = $uname ; $tab_name = uc( $arg ) ;
     }

   return ( $owner, $tab_name ) ;
  }


sub get_ra_Tcols
  {
   my ( $owner, $table_name ) = @_ ;
   my ( $col_name, $data_type, $mandatory ) ;
   my @Tcols ;

   my $c_col = $Lda->prepare("
select
  a.COLUMN_NAME,
  case
    when a.DATA_TYPE = 'NUMBER'
    then
      case
        when a.DATA_PRECISION is null
        then a.DATA_TYPE
        else 
          case
            when a.DATA_SCALE = 0
            then a.DATA_TYPE||'('||to_char( a.DATA_PRECISION )||')'
            else a.DATA_TYPE||'('||to_char( a.DATA_PRECISION )||','||to_char( a.DATA_SCALE )||')'
          end
      end
    when a.DATA_TYPE = 'CHAR'
    then
      case
        when a.DATA_LENGTH = 1
        then a.DATA_TYPE
        else a.DATA_TYPE||'('||to_char( a.DATA_LENGTH )||')'
      end
    when a.DATA_TYPE like 'VARCHAR%'
    then a.DATA_TYPE||'('||to_char( a.DATA_LENGTH )||')'
    else a.DATA_TYPE
  end as DATA_TYPE,
  case when a.NULLABLE = 'N' then 'Y' end as MAN
from
  ALL_TAB_COLUMNS a
where
      a.OWNER = ?
  and a.TABLE_NAME = ?
order by
  a.COLUMN_ID") ;

   $c_col->execute( $owner, $table_name ) ;
   while( ( $col_name, $data_type, $mandatory ) = $c_col->fetchrow_array())
     {
      unless( $mandatory ) { $mandatory = ''}
      push( @Tcols, [ $col_name, $data_type, $mandatory ] ) ;
     }

   return \@Tcols ;
  }


sub get_sql1
  {
   my ( $owner, $table_name, $ra_Tcols ) = @_ ;
   my ( $ra_Col, $sql, $col_name, $pref ) ;
   my $ix = 0 ;

   $sql = 'select'."\n".
          '  count(*) as CNT_ALL' ;

   foreach $ra_Col ( @$ra_Tcols )
     {
      $col_name = $ra_Col->[0] ;
      ++$ix ;
      $pref = 'C'. $ix .'_' ;
      if( $ra_Col->[1] =~ /(LOB|LONG)/ )
        {
         $sql .= ",\n".
                 '  -1   as '. $pref .'CA,'."\n".
                 '  -1   as '. $pref .'CD,'."\n".
                 '  NULL as '. $pref .'MI,'."\n".
                 '  NULL as '. $pref .'MA' ;
        }
      else
        {
         $sql .= ",\n".
                 '  count('. $col_name .') as '. $pref .'CA,'."\n".
                 '  count(distinct '. $col_name .') as '. $pref .'CD,'."\n".
                 '  min('. $col_name .') as '. $pref .'MI,'."\n".
                 '  max('. $col_name .') as '. $pref .'MA' ;
        }
     }

   $sql .= "\n".
           'from'."\n".
           '  '. lc( $owner ) .'.'. $table_name ;

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

      $cn = ($ca >= 0 ) ? ($cnt_all - $ca) : -1 ;
      $un = ($ca >= 0 && ($cnt_all - $cn) == $cd && $cd > 0 ) ? 'Y':'' ;
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
   my ( $owner, $table_name ) = @_ ;
   my ( $ra_Tcols, $ra_Col ) ;
   my ( $ra_Tva1,  $ra_Va1 ) ;
   my ( $sql, $col_cnt ) ;
   my $ix ;

   $ra_Tcols = get_ra_Tcols( $owner, $table_name ) ;
   $col_cnt = @$ra_Tcols ; # -- count of columns

#  foreach $ra_Col ( @$ra_Tcols )
#    {
#     print sprintf("%-30s", $ra_Col->[0] ) .' '.
#           sprintf("%-15s", $ra_Col->[1] ) .' '.
#           $ra_Col->[2] ."\n" ;
#    }

   $sql = get_sql1( $owner, $table_name, $ra_Tcols ) ;
   $ra_Tva1 = run_sql1( $sql, $col_cnt ) ;

   for( $ix = 0 ; $ix < $col_cnt ; ++$ix )
     {
      $ra_Col = $ra_Tcols->[ $ix ] ;
      $ra_Va1 = $ra_Tva1->[ $ix ] ;

      print join(";", ( $owner,
                        $table_name,
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

