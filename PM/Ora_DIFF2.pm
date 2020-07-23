# Ora_DIFF2.pm
#
# 2019-04-26
# SQL DIFF commands generation
# Usage description is at main procedure print_sql_diff()

package Ora_DIFF2 ;

use strict ;
use warnings ;
use integer ;
# use Data::Dumper ;
use DBI ;
use Exporter ;

@Ora_DIFF2::ISA    = qw(Exporter) ;
@Ora_DIFF2::EXPORT = qw(&set_owner_default &print_sql_diff) ;

# constants for $ra_Tab_cols
use constant {
  CN   => 0, # column name
  NU   => 1, # nullable (Y/N)
  TP1  => 2, # data type abbrev
  TP2  => 3, # data type
  TP3  => 4  # optional type (K: key, O: omitted)
} ;

# NVL default values
my %h_NVL_def = ('C' => "'#'",                # CHAR / VARCHAR
                 'N' => ' -1 ',               # NUMBER
                 'D' => " DATE '1111-11-11'", # DATE
                 'T' => " TIMESTAMP '1111-11-11 11:11:11'" # TIMESTAMP
                ) ;

my $Owner_DEFAULT = '<undef>' ; # Oracle Default Schema Owner


sub test_var_ref_LDA
  # test if paramerer $lda is "oracle logon data area"
  {
   my ( $lda ) = @_ ;
   my ( $tp, $rc ) ;

   $tp = ref( $lda ) ;
   if( $tp =~ /^DBI/ ) { $rc = 1 }
   else
     {
      unless( $tp ) { $tp = 'scalar' }
      print STDERR 'Error on '. (caller(1))[3] .'() parameter: '. $tp .' where DBI::db expected.'."\n" ;
      $rc = 0 ; # false
     }
   return $rc ;
  }


sub set_owner_default
  # Set $Owner_DEFAULT (default Oracle USERNAME)
  # Parameters:
  #   $lda       : Oracle Login Data Area (Oracle session handler)
  #   $owner     : [optional] Oracle username (if empty - current USER used)
  #   $print_flg : [optional] print flag (0: false, undef or <>0: true)
  {
   my ( $lda, $owner, $print_flg ) = @_ ;
   my ( $err_msg, $rc ) ;

   if( ( defined $lda ) && ref($lda) =~ /^DBI/ )
     {
      if( defined $owner && $owner =~ /^[A-Za-z]+/ )
        {
         # $owner looks like as USERNAME => test if exists any accessible datatabase object
         my $name ;
         my $c1 = $lda->prepare('select OBJECT_NAME from ALL_OBJECTS where OWNER = upper(?) and ROWNUM < 2') ;
         $c1->execute( $owner ) ;
         ( $name ) = $c1->fetchrow_array() ;
         $c1->finish() ;
         if( $name )  { $Owner_DEFAULT = lc($owner) }
         else
           {
            $Owner_DEFAULT = '<undef>' ;
            $err_msg = ' ('. $owner .' - USERNAME with no accessible database object).' ;
           }
        }
      else
        {
         # $owner taken from current session
         my $c1 = $lda->prepare('select lower(USER) from DUAL') ;
         $c1->execute() ;
         ( $Owner_DEFAULT ) = $c1->fetchrow_array() ;
         $c1->finish() ;
        }
     }
   else
     {
      # it is impossible to validate $owner
      $Owner_DEFAULT = '<undef>' ;
      $err_msg = ' (no valid Oracle Logon Data Area).' ;
     }

   if( ! defined $print_flg || $print_flg ) { print '-- DEFALUT OWNER: '. $Owner_DEFAULT ."\n" }

   if( defined $Owner_DEFAULT && $Owner_DEFAULT =~ /^[a-z]+/ )
     {
      $rc = 1 ; # true (OK)
     }
   else
     {
      $rc = 0 ; # false
      print STDERR '-- ERROR: default OWNER is unset'. $err_msg ."\n" ;
     }
   return $rc ;
  }


sub get_tab_cols
  # get table columns definition
  {
   my ( $lda, $owner, $table_name ) = @_ ;
   my ( $column_name, $nullable, $tp_1, $tp_2 ) ;
   my ( @a_Tab_cols, %h_Col_ix ) ;
   my ( $index, $rc ) ;

   my $crs = $lda->prepare( q{
select
  COLUMN_NAME,
  NULLABLE,
  case
    when regexp_like( DATA_TYPE,'CHAR')       then 'C'
    when DATA_TYPE = 'NUMBER'                 then 'N'
    when DATA_TYPE = 'DATE'                   then 'D'
    when regexp_like( DATA_TYPE,'^TIMESTAMP') then 'T'
                                              else 'x'
  end as TP_1,
  regexp_substr( DATA_TYPE,'^\w+') as TP_2
from
  ALL_TAB_COLUMNS
where
  OWNER = ?
  and TABLE_NAME = ?
order by
  COLUMN_ID
} ) ;
   $crs->execute( $owner, $table_name ) ;
   $rc = 1 ; # true (OK)
   $index = 0 ;
   while( ( $column_name, $nullable, $tp_1, $tp_2 ) = $crs->fetchrow_array() )
     {
      push( @a_Tab_cols, [ $column_name, # column name
                           $nullable,    # Y/N
                           $tp_1,        # data type abbrev
                           $tp_2,        # data type
                           '-']          # default for optional type (K: key, O: omitted)
          ) ;
      $h_Col_ix{ $column_name } = $index ; # column index to @a_Tab_cols
      $index++ ;
     }
   if( $index == 0 )
     {
      $rc = 0 ; # false
      print STDERR '-- ERROR: '. $owner .'.'. $table_name . ' - invalid table name.'."\n" ;
     }
   return ( $rc, \@a_Tab_cols, \%h_Col_ix ) ;
  }


sub test_cols
  # test if column_names at list $ra_cols exist at hash $rh_Col_ix
  # hash $rh_Col_ix generated at get_tab_cols()
  {
   my ( $rh_Col_ix, $ra_cols ) = @_ ;
   my ( $col, $cnt_err ) ;

   $cnt_err = 0 ;
   foreach $col ( @$ra_cols )
     {
      if( ! exists $rh_Col_ix->{ $col } )
        {
         print '-- ERROR: '. $col .' - invalid column name.'."\n" ; $cnt_err++ ;
        }
     }
   return $cnt_err ;
  }


sub cols_SET_tp_3
  # set "tp_3" for selected columns
  {
   my ( $ra_Tab_cols, $rh_Col_ix, $ra_cols, $tp_3 ) = @_ ;
   my ( $col, $ix ) ;

   foreach $col ( @$ra_cols )
     {
      if( exists $rh_Col_ix->{ $col } )
        {
         $ix = $rh_Col_ix->{ $col } ; $ra_Tab_cols->[ $ix ][ TP3 ] = $tp_3 ;
        }
     }
  }


sub get_COL_EXPR
  # derive COL_EXPR (alias=x, nvl() if nullable) from COL_NAME (column name)
  # key column => set "tp_3" at $ra_Tab_cols
  {
   my ( $tab_name, $col_name, $ra_Tab_cols, $rh_Col_ix, $tp_3 ) = @_ ;
   my ( $col_name2, $nullable, $tp_1, $tp_2 ) ;
   my ( $index, $col_expr, $nvl_df, $err_cnt ) ;

   $err_cnt = 0 ;
   # default expression
   $col_expr = 'x.'. $col_name ;

   if( exists $rh_Col_ix->{ $col_name } )
     {
      $index = $rh_Col_ix->{ $col_name } ;
      # key column mark
      if( defined $tp_3 && $tp_3 eq 'K') { $ra_Tab_cols->[ $index ][ TP3 ] = $tp_3 }

      # get column detail
      ( $col_name2, $nullable, $tp_1, $tp_2 ) = @{$ra_Tab_cols->[ $index ]} ;
      if( ! defined $col_name2 || $col_name2 ne $col_name )
        {
         print '-- INTERNAL ERROR: '. $tab_name .': ix='. $index .', col1='. $col_name .', col2='. $col_name2 ."\n" ;
         $err_cnt++ ;
        }
      elsif( $nullable eq 'Y' )
        {
         if( exists $h_NVL_def{ $tp_1 } )
           {
            $nvl_df = $h_NVL_def{ $tp_1 } ;
            # nullable column expression
            $col_expr = 'nvl( '. $col_expr .','. $nvl_df .')' ;
           }
         else
           {
            print '-- ERROR: '. $tab_name .'.'. lc( $col_name ) .' - '. $tp_2 .' - unexpected datatype.'."\n" ;
            $err_cnt++ ;
           }
        }
     }
   else
     {
      # if @$ra_Tab_cols is empty => $tab_name is invalid (and error message is printed yet)
      if( scalar @$ra_Tab_cols > 0 )
        { print '-- ERROR: '. $tab_name .'.'. lc( $col_name ) .' - invalid column name .'."\n" }
      $err_cnt++ ;
     }

   return ( $col_expr, $err_cnt ) ;
  }


sub get_cols_expr_key
  # generates expresion list for key-columns
  {
   my ( $tab_name, $ra_Tab_cols, $rh_Col_ix, $ra_cols_key ) = @_ ;
   my ( @a_Cols_expr_key, $col_name, $col_expr, $err_cnt, $err_cnt_ALL ) ;

   $err_cnt_ALL = 0 ;
   foreach $col_name ( @$ra_cols_key )
     {
      ( $col_expr, $err_cnt ) = get_COL_EXPR( $tab_name, $col_name, $ra_Tab_cols, $rh_Col_ix,'K') ;
      $err_cnt_ALL += $err_cnt ;

      push( @a_Cols_expr_key, $col_expr ) ;
     }

   return ( \@a_Cols_expr_key, $err_cnt_ALL ) ;
  }


sub get_OWNER_and_TABLE_NAME
  # get owner.TABLE_NAME from $tab param ($tab: <[owner.]table_name>
  # using default owner ($Owner_DEFAULT) if necessary
  {
   my ( $tab ) = @_ ;
   my ( $pos, $owner, $table ) ;

   if( ! $tab ) { $tab = '' }
   if( ref( $tab ) eq '' )
     {
      $pos = index( $tab,'.') ;
      if( $pos == -1 )
        {
         $owner = $Owner_DEFAULT ;
         $table = uc( $tab ) ;
        }
      else
        {
         $owner = ( $pos > 0 ) ? lc( substr( $tab, 0, $pos )) : $Owner_DEFAULT ;
         $table = uc( substr( $tab, $pos+1 )) ;
        }
     }
   else
     { $owner = $table = '' }

   # print '** '. ((defined $tab)   ? $tab   : '<undef>') .' -> '.
   #              ((defined $owner) ? $owner : '<undef>') .'.'.
   #              ((defined $table) ? $table : '<undef>') ."\n" ;
   return( $owner, $table ) ;
  }


sub get_OWNERs_and_TABLE_NAMEs
  # call get_OWNER_and_TABLE_NAME() for table_1 and table_2
  # table_1 is mandatory, so print error message if is not accessible
  {
   my ( $tab1, $tab2 ) = @_ ;
   my ( $owner_1, $owner_2, $table_1, $table_2 ) ;

   ( $owner_1, $table_1 ) = get_OWNER_and_TABLE_NAME( $tab1 ) ;
   ( $owner_2, $table_2 ) = get_OWNER_and_TABLE_NAME( $tab2 ) ;
   if( $table_1 )
     {
      if( ! $table_2 ) { $owner_2 = $owner_1 ; $table_2 = $table_1 ; }
     }
   else
     {
      print STDERR 'Error on '. (caller(1))[3] .': table_1 ='. ((defined $tab1 ) ? $tab1 : '<undef>') .".\n" ;
     }

   return ( $owner_1, $table_1, $owner_2, $table_2 ) ;
  }


sub get_cols_UNIQ_and_UC_single
  # Convert input column list to output column list
  # - column names to uppercase
  # - remove duplicate column names
  {
   my ( $label, $ra_cols_in, $ra_cols_out, $rh_cols ) = @_ ;
   my ( $col_name, $rc ) ;

   if( defined $ra_cols_in && ref( $ra_cols_in ) eq 'ARRAY')
     {
      $rc = 1 ;
      for $col_name ( @$ra_cols_in )
        {
         $col_name = uc( $col_name ) ;
         if( ! exists $rh_cols->{ $col_name } )
           {
            push( @$ra_cols_out, $col_name ) ; $rh_cols->{ $col_name } = 1 ;
           }
         else
           { print STDERR 'Warning: '. $col_name .' - duplicate column name (tp3='. $label .")\n" }
        }
     }
   else
     { $rc = 0 }

   return $rc ;
  }


sub get_cols_UNIQ_and_UC
  # Convert input column lists to output column lists
  # cols_key (key-columns) is mandatory
  # cols_oth (other columns), cols_dif (diff cols.) are optional
  # param. $man_flg used for cols_dif only (and only prints warning if cols_dif is empty)
  {
   my ( $ra_cols_key_in, $ra_cols_oth_in, $ra_cols_dif_in, $man_flg ) = @_ ;
   my @a_cols_key = () ;
   my @a_cols_oth = () ;
   my @a_cols_dif = () ;
   my %h_cols     = () ;
   my $rc ;

   # test for key-columns (key columns - mandatory)
   $rc = get_cols_UNIQ_and_UC_single('K', $ra_cols_key_in, \@a_cols_key, \%h_cols ) ;
   if( $rc == 0 ) { print STDERR 'Error on '. (caller(1))[3] .': $ra_cols_key_in - invalid value.'."\n" }

   # test for columns other
   get_cols_UNIQ_and_UC_single('O', $ra_cols_oth_in, \@a_cols_oth, \%h_cols ) ;

   # test for columns diff
   get_cols_UNIQ_and_UC_single('D', $ra_cols_dif_in, \@a_cols_dif, \%h_cols ) ;

   # error message only (caller(1): procedure name called this procedure)
   if( $rc != 0 && scalar @a_cols_key == 0 )
     {
      $rc = 0 ; # false
      print STDERR 'Error on '. (caller(1))[3] .': $ra_cols_key_in - empty.'."\n" ;
     }
   if( defined $man_flg && $man_flg eq 'Y' && scalar @a_cols_dif == 0 )
     {
      print STDERR 'Warning: '. (caller(1))[3] .': $ra_cols_dif_in (diff.cols.) is empty (used all testable cols.instead).'."\n" ;
     }

   return ( $rc, \@a_cols_key, \@a_cols_oth, \@a_cols_dif ) ;
  }


sub push_XTABx
  # key-differences SQL: X_TAB(n) virtual view
  {
   my ( $ra_sql, $seq, $star, $own, $tab, $alias, $ra_cols_key, $ra_cols_oth, $query, $last_flg ) = @_ ;

   push( @$ra_sql,'X_TAB'. $seq ) ;
   push( @$ra_sql,'as') ;
   if( $star ne '*')
     {
      push( @$ra_sql,'  ( select '. $seq .' as XDF_SEQNO, '.
                          $alias .'.'. join( (', '. $alias .'.'), ( @$ra_cols_key, @$ra_cols_oth )) ) ;
     }
   else
     {
      push( @$ra_sql,'  ( select '. $alias .'.*') ;
     }
   push( @$ra_sql,'    from   '. $own .'.'. $tab .' '. $alias ) ;
   if( $query )
     {
      $query =~ s/\bx\./${alias}./g ;
      push( @$ra_sql,'    where  '. $query ) ;
     }
   push( @$ra_sql,'  )'. ((! $last_flg) ? ',' : '' )) ;
  }


sub push_XTABx_DF
  # key-differences SQL: X_TAB(n)_DIFF virtual view
  {
   my ( $ra_sql, $seq1, $own, $tab, $ali_1, $ali_2, $ra_cols_key, $ra_cols_expr_key, $ra_cols_oth, $last_flg ) = @_ ;
   my ( $seq2, $expr_1, $expr_2, $ix, $cnt ) ;
   my ( $begin ) ;

   $seq2 = ( $seq1 == 1 ) ? 2 : 1 ;

   push( @$ra_sql,'X_TAB'. $seq1 .'_DIFF') ;
   push( @$ra_sql,'as') ;
   push( @$ra_sql,'  ( select') ;
   push( @$ra_sql,'      '. $ali_1 .'.XDF_SEQNO,') ;
   push( @$ra_sql,q{      '}. $own .q{' as XDF_OWNER,} ) ;
   push( @$ra_sql,q{      '}. $tab .q{' as XDF_TABLE_NAME,} ) ;
   push( @$ra_sql,'      '. $ali_1 .'.'. join( (', '. $ali_1 .'.'), @$ra_cols_key) .
                  (($ra_cols_oth->[0]) ? ',':'') .' -- key cols'
       ) ;
   if( $ra_cols_oth->[0] )
     {
      push( @$ra_sql,'      '. $ali_1 .'.'. join( (', '. $ali_1 .'.'), @$ra_cols_oth) ) ;
     }
   push( @$ra_sql,'    from') ;
   push( @$ra_sql,'      X_TAB'. $seq1 .' '. $ali_1 ) ;
   push( @$ra_sql,'      left outer join X_TAB'. $seq2 .' '. $ali_2 ) ;

   $cnt = scalar @$ra_cols_expr_key ;
   for( $ix = 0 ; $ix < $cnt ; )
     {
      $expr_1 = $ra_cols_expr_key->[$ix] ;
      $expr_2 = $expr_1 ;
      $expr_1 =~ s/\bx\./${ali_1}./g ;
      $expr_2 =~ s/\bx\./${ali_2}./g ;
      $begin = ($ix == 0 ) ? (' ' x 6). 'on ( ' : (' ' x 10 ). ' and ' ;
      ++$ix ;
      push( @$ra_sql, $begin . $expr_1 .' = '. $expr_2 . (($ix < $cnt) ? '' : ' )') ) ;
     }
   push( @$ra_sql,'    where') ;
   push( @$ra_sql,'      '. $ali_2 .'.XDF_SEQNO is null') ;
   push( @$ra_sql,'  )'. ((! $last_flg) ? ',' : '' )) ;
  }


sub get_sql_df_KEY_CNT
  # key-differences SQL
  {
   my ( $lda, $owner_1, $owner_2, $table_1, $table_2, $q1, $q2,
        $ra_cols_key, $ra_cols_expr_key, $ra_cols_out,
        $ra_Tab_cols, $rh_Col_ix ) = @_ ;
   my ( @a_sql ) ;

   push( @a_sql,'with') ;
   #         ( $ra_sql, $seq, $star, $own,     $tab,     $alias, $ra_cols_key, $ra_cols_oth, $query, $last_flg )
   push_XTABx( \@a_sql, 1,    '-',   $owner_1, $table_1,'a',     $ra_cols_key, $ra_cols_out, $q1,    0 )  ;
   push_XTABx( \@a_sql, 2,    '-',   $owner_2, $table_2,'b',     $ra_cols_key, $ra_cols_out, $q2,    0 )  ;

   push_XTABx_DF( \@a_sql, 1, $owner_1, $table_1,'c','d', $ra_cols_key, $ra_cols_expr_key, $ra_cols_out, 0 ) ;
   push_XTABx_DF( \@a_sql, 2, $owner_2, $table_2,'e','f', $ra_cols_key, $ra_cols_expr_key, $ra_cols_out, 0 ) ;

   push( @a_sql,'X_DIFF') ;
   push( @a_sql,'as') ;
   push( @a_sql,'  ( select * from X_TAB1_DIFF') ;
   push( @a_sql,'    union all') ;
   push( @a_sql,'    select * from X_TAB2_DIFF') ;
   push( @a_sql,'  )') ;
   push( @a_sql,'-- select * from X_TAB1_DIFF') ;
   push( @a_sql,'select') ;
   push( @a_sql,'  XDF_SEQNO,') ;
   push( @a_sql,'  XDF_OWNER,') ;
   push( @a_sql,'  XDF_TABLE_NAME,') ;
   push( @a_sql,'  count(XDF_SEQNO) as CNT') ;
   push( @a_sql,'from') ;
   push( @a_sql,'  X_DIFF') ;
   push( @a_sql,'group by') ;
   push( @a_sql,'  XDF_SEQNO,') ;
   push( @a_sql,'  XDF_OWNER,') ;
   push( @a_sql,'  XDF_TABLE_NAME') ;

   return \@a_sql ;
  }


sub get_sql_df_COL_CNT
  # col-differences counts SQL
  {
   my ( $lda, $owner_1, $owner_2, $table_1, $table_2, $q1, $q2,
        $ra_cols_key, $ra_cols_expr_key, $ra_no_key_cols,
        $ra_Tab_cols, $rh_Col_ix ) = @_ ;
   my ( @a_sql, $cnt, $ix, $col_ix, $last_ch, $txt ) ;
   my ( $ali_1, $ali_2, $expr_1, $expr_2 ) ;
   my ( $col_name, $nullable, $tp_1, $tp_2, $tp_3 ) ;

   # with clause
   push( @a_sql,'with') ;
   #         ( $ra_sql, $seq, $star, $own,     $tab,     $alias, $ra_cols_key, $ra_cols_oth, $query, $last_flg )
   push_XTABx( \@a_sql, 1,    '*',   $owner_1, $table_1, 'a',    undef       , undef,        $q1,    0 )  ;
   push_XTABx( \@a_sql, 2,    '*',   $owner_2, $table_2, 'b',    undef       , undef,        $q2,    1 )  ;
   # select clause
   push( @a_sql,'select') ;
   for $col_name ( @$ra_cols_key ) { push( @a_sql,"  'K' as ". $col_name .',') }
   push( @a_sql,'  count(1) as CNT_ALL,') ;
   $ali_1 = 'c' ; $ali_2 = 'd' ;

   $cnt = scalar @$ra_no_key_cols ;
   $last_ch = ',' ;
   for( $ix = 0 ; $ix < $cnt ; )
     {
      $col_ix = $rh_Col_ix->{ $ra_no_key_cols->[ $ix ] } ;
      ( $col_name, $nullable, $tp_1, $tp_2, $tp_3 ) = @{$ra_Tab_cols->[$col_ix]} ;
      if( $col_name ne $ra_no_key_cols->[ $ix ] )
        { print STDERR 'Internal ERROR '. $col_name .'<>'. $ra_no_key_cols->[ $ix ] .' for '. $owner_1 .'.'. $table_1 ."\n" }
      $expr_1 = $ali_1 .'.'. $col_name ;
      $expr_2 = $ali_2 .'.'. $col_name ;
      ++$ix ;
      if( $ix == $cnt ) { $last_ch = '' }

      if( $tp_3 eq 'O')
        { push( @a_sql,"  's' as ". $col_name . $last_ch ) } # omitted column (no test)
      else
        {
         push( @a_sql,'  sum( case') ;
         if( $nullable eq 'Y')
           {
            push( @a_sql, (' ' x  9) .'when ( '. $expr_1 .' is null and '. $expr_2 .' is null )') ;
            push( @a_sql, (' ' x 14) .'or ' . $expr_1 .' = '. $expr_2 ) ;
           }
         else
           {
            push( @a_sql, (' ' x  9) .'when ' . $expr_1 .' = '. $expr_2 ) ;
           }
         push( @a_sql, (' ' x  9) .'then NULL') ;
         push( @a_sql, (' ' x  9) .'else 1') ;
         push( @a_sql, (' ' x  7) .'end ) as '. $col_name . $last_ch ) ;
        }
     }
   # from clause
   push( @a_sql,'from') ;
   push( @a_sql,'  X_TAB1 '. $ali_1 ) ;
   push( @a_sql,'  inner join X_TAB2 '. $ali_2 ) ;
   # join condition
   $cnt = scalar @$ra_cols_expr_key ;
   for( $ix = 0 ; $ix < $cnt ; )
     {
      $expr_1 = $ra_cols_expr_key->[$ix] ;
      $expr_2 = $expr_1 ;
      $expr_1 =~ s/\bx\./${ali_1}./g ;
      $expr_2 =~ s/\bx\./${ali_2}./g ;
      $txt = ($ix == 0 ) ? '  on ( ' : (' ' x 7 ). 'and ' ;
      ++$ix ;
      push( @a_sql, $txt . $expr_1 .' = '. $expr_2 . (($ix < $cnt) ? '' : ' )') ) ;
     }

   return \@a_sql ;
  }


sub get_sql_df_COL_VAL
  # detail columns-differences SQL
  {
   my ( $lda, $owner_1, $owner_2, $table_1, $table_2, $q1, $q2,
        $ra_cols_key, $ra_cols_expr_key, $ra_cols_dif,
        $ra_Tab_cols, $rh_Col_ix ) = @_ ;
   my ( @a_sql, @a_cols_dif ) ;
   my ( $col_name, $ali_1, $ali_2, $ix, $col_ix, $cnt, $last_ch, $txt, $tp_3 ) ;
   my ( $expr_1, $expr_2, $nullable, $err_cnt ) ;

   for $col_name ( @$ra_cols_dif )
     {
      if( exists $rh_Col_ix->{ $col_name } )
        {
         $col_ix = $rh_Col_ix->{ $col_name } ; $tp_3 = $ra_Tab_cols->[$col_ix][ TP3 ] ;
         if( $tp_3 ne 'K' && $tp_3 ne 'O' ) { push( @a_cols_dif, $col_name ) }
        }
     }
   $cnt = scalar @a_cols_dif ;
   if( $cnt == 0 )
     {
      print '-- Warning: '. $owner_1 .'.'. $table_1 .' has no columns for detail diff.'."\n" ;
      return ;
     }
   push( @a_sql,'with') ;
   #         ( $ra_sql, $seq, $star, $own,     $tab,     $alias, $ra_cols_key, $ra_cols_oth, $query, $last_flg )
   push_XTABx( \@a_sql, 1,    '-',   $owner_1, $table_1,'a',     $ra_cols_key, \@a_cols_dif,        $q1,    0 )  ;
   push_XTABx( \@a_sql, 2,    '-',   $owner_2, $table_2,'b',     $ra_cols_key, \@a_cols_dif,        $q2,    1 )  ;

   $ali_1 = 'c' ; $ali_2 = 'd' ; $last_ch = ',' ;
   push( @a_sql,'select') ;
   for $col_name ( @$ra_cols_key ) { push( @a_sql,'  '. $ali_1 .'.'. $col_name .',') }
   for( $ix = 0 ; $ix < $cnt ; )
     {
      $col_name = $a_cols_dif[ $ix ] ;
      $col_ix = $rh_Col_ix->{ $col_name } ;
      $nullable = $ra_Tab_cols->[ $col_ix ][ NU ] ;
      $ix++ ;
      if( $ix == $cnt ) { $last_ch = '' }
      $expr_1 = $ali_1 .'.'. $col_name ;
      $expr_2 = $ali_2 .'.'. $col_name ;
      push( @a_sql,'  '. $expr_1 .' as '. substr( $col_name, 0, 28 ) .'_1,') ;
      push( @a_sql,'  '. $expr_2 .' as '. substr( $col_name, 0, 28 ) .'_2,') ;
      push( @a_sql,'  case') ;
      if( $nullable eq 'Y')
        {
         push( @a_sql, (' ' x 4) .'when ( '. $expr_1 .' is null and '. $expr_2 .' is null )') ;
         push( @a_sql, (' ' x 9) .'or ' . $expr_1 .' = '. $expr_2 ) ;
        }
      else
        {
         push( @a_sql, (' ' x  4) .'when ' . $expr_1 .' = '. $expr_2 ) ;
        }
      push( @a_sql, (' ' x  4) .'then NULL') ;
      push( @a_sql, (' ' x  4) ."else 'Y'") ;
      push( @a_sql, '  end as FLG_NE_'. $ix . $last_ch ) ;
     }
   # from clause
   push( @a_sql,'from') ;
   push( @a_sql,'  X_TAB1 '. $ali_1 ) ;
   push( @a_sql,'  inner join X_TAB2 '. $ali_2 ) ;
   # join condition
   $cnt = scalar @$ra_cols_expr_key ;
   for( $ix = 0 ; $ix < $cnt ; )
     {
      $expr_1 = $ra_cols_expr_key->[$ix] ;
      $expr_2 = $expr_1 ;
      $expr_1 =~ s/\bx\./${ali_1}./g ;
      $expr_2 =~ s/\bx\./${ali_2}./g ;
      $txt = ($ix == 0 ) ? '  on ( ' : (' ' x 7 ). 'and ' ;
      ++$ix ;
      push( @a_sql, $txt . $expr_1 .' = '. $expr_2 . (($ix < $cnt) ? '' : ' )') ) ;
     }
   # where clause
   push( @a_sql,'where') ;
   $cnt = scalar @a_cols_dif ;
   for( $ix = 0 ; $ix < $cnt ; ++$ix )
     {
      $col_name = $a_cols_dif[ $ix ] ;
      ( $expr_1, $err_cnt ) = get_COL_EXPR( $table_1, $col_name, $ra_Tab_cols, $rh_Col_ix ) ;
      $expr_2 = $expr_1 ;
      $expr_1 =~ s/\bx\./${ali_1}./g ;
      $expr_2 =~ s/\bx\./${ali_2}./g ;
      $txt = ($ix == 0 ) ? '  ' : '  or ' ;
      push( @a_sql, $txt . $expr_1 .' != '. $expr_2 ) ;
     }

   return \@a_sql ;
  }


sub get_TABx_label
  # print table label only
  {
   my ( $seq, $start_row, $own, $tab, $query ) = @_ ;
   return $start_row .' tab'. $seq .': '. $own .'.'. $tab .
          (( defined( $query ) && length( $query ) > 0 ) ? ' ('. $query .')' : '') ;
  }


sub nvl
  {
   my ( $val ) = @_ ;

   if( ref($val) eq '')
     { return ((defined $val) ? $val : '') }
   else
     { return ((defined $val) ? join(', ', @$val ) : '') }
  }


sub print_sql_diff
  # Generate and print SQL for key-differences between TABLE_1 (primary) and TABLE_2 (secondary)
  # Parameters:
  #   $fce        - generated sql-type: 1: KEY DIFF CNT, 2: COLS DIFF CNT, 3: COLS DIFF DETAIL
  #   $prompt_flg - if true (!=0) => generate sqlplus prompt command instead of comment
  #   $lda        - Oracle Logon Data Area (Oracle session handler)
  #   $tab1       - <[owner.]table_name> - primary table (column list taken for primary table)
  #                 (if owner omitted - it is necessary to set up the DEFAULT Owner by procedure set_owner_default() )
  #   $tab2       - (optional) <[owner.]table_name> (or empty) - secondary table (if empty => $tab2 = $tab1)
  #   $q1         - (optional) query1 - filter condition for primary table (use alias x.)
  #   $q2         - (optional) query2 - filter condition for secondary table (use alias x.)
  #   $ra_cols_key_in - reference to key-columns list (unique key used at joins)
  #   $ra_cols_oth_in - (optional) reference to other-columns - meaning depends on $fce
  #                     (fce=1: added columns, fce=2,3: omitted columns)
  #   $ra_cols_dif_in - (optional) columns for diff-detail, for fce=3 only
  {
   my ( $fce, $prompt_flg, $lda, $tab1, $tab2, $q1, $q2, $ra_cols_key_in, $ra_cols_oth_in, $ra_cols_dif_in ) = @_ ;
   my ( $owner_1, $owner_2, $table_1, $table_2, $err_cnt1, $rc1, $rc2 ) ;
   my ( $ra_cols_key, $ra_cols_expr_key, $ra_cols_oth, $ra_cols_dif ) ;
   my ( $ra_Tab_cols, $rh_Col_ix ) ;
   my ( $ra_sql, $txt, $start_row ) ;

# print '>> tab_1 = '. nvl( $tab1 ) ."\n" ;
# print '>> tab_2 = '. nvl( $tab2 ) ."\n" ;
# print '>> q_1   = '. nvl( $q1 ) ."\n" ;
# print '>> q_2   = '. nvl( $q2 ) ."\n" ;
# print '>> c_key = '. nvl( $ra_cols_key_in ) ."\n" ;
# print '>> c_oth = '. nvl( $ra_cols_oth_in ) ."\n" ;

   # init and test input parameters (return if any fatal error occurred)
   if( test_var_ref_LDA( $lda ) == 0 ) { return }
   ( $owner_1, $table_1,
     $owner_2, $table_2 ) = get_OWNERs_and_TABLE_NAMEs( $tab1, $tab2 ) ;
   unless( $table_1 ) { return }
   # col_name DISTINCT only without existence test in the database
   ( $rc1, $ra_cols_key, $ra_cols_oth,
     $ra_cols_dif ) = get_cols_UNIQ_and_UC( $ra_cols_key_in, $ra_cols_oth_in, $ra_cols_dif_in,
                                            (($fce == 3) ? 'Y':'N') # mandatory flag
                                          ) ;
   # table structure from database
   ( $rc2,         # return code (0: false)
     $ra_Tab_cols, # table columns properties
     $rh_Col_ix ) = get_tab_cols( $lda, uc($owner_1), $table_1 ) ;

   if( $rc1 == 0 || $rc2 == 0 ) { return }
   # sqlplus prompt or comment
   $start_row = ( defined $prompt_flg && $prompt_flg) ? 'prompt' : '--' ;

   # print head
   print get_TABx_label( 1, $start_row, $owner_1, $table_1, $q1 ) ."\n".
         get_TABx_label( 2, $start_row, $owner_2, $table_2, $q2 ) ."\n".
         $start_row .' key:  '. join(', ', @$ra_cols_key ) ."\n" ;
   if( scalar @$ra_cols_oth > 0 && $fce >= 1 && $fce <= 3 )
     {
      $txt = (($fce == 1 ) ? 'add' : 'omt') ;
      print $start_row .' '. $txt .':  '. join(', ', @$ra_cols_oth ) ."\n" ;
     }

   # expression for columns, set K columns at ra_Tab_cols, print errors
   ( $ra_cols_expr_key, $err_cnt1 ) = get_cols_expr_key( $table_1, $ra_Tab_cols, $rh_Col_ix, $ra_cols_key ) ;
   # column names validation with hash $rh_Col_ix obtained from database
   if(( $err_cnt1
        + test_cols( $rh_Col_ix, $ra_cols_oth )
        + test_cols( $rh_Col_ix, $ra_cols_dif )) > 0 ) { print "\n" } # new-line after err-messages only

   # generate SQL
   if( $fce == 1 ) # KEY diff count SQL
     {
      # $ra_cols_oth - added columns for fce=1
      $ra_sql = get_sql_df_KEY_CNT( $lda, $owner_1, $owner_2, $table_1, $table_2, $q1, $q2,
                                    $ra_cols_key, $ra_cols_expr_key, $ra_cols_oth,
                                    $ra_Tab_cols, $rh_Col_ix ) ;
     }
   elsif( $fce == 2 ) # COL diff count SQL
     {
      my ( $ra_Col_prop, @a_no_key_cols ) ;

      # set omitted coluns at $ra_Tab_cols ($ra_cols_oth - omitted columns for fce=2)
      if( scalar @$ra_cols_oth > 0 ) { cols_SET_tp_3( $ra_Tab_cols, $rh_Col_ix, $ra_cols_oth,'O') }
      # list of all no key columns
      for $ra_Col_prop ( @$ra_Tab_cols )
        {
         if( $ra_Col_prop->[ TP3 ] ne 'K' ) { push( @a_no_key_cols, $ra_Col_prop->[ 0 ] ) }
        }
      # for execute SQL by sql_diff_RUN_2.pl
      print '-- T '. $owner_1 .'.'. $table_1 .' '. $owner_2 .'.'. $table_2 ."\n".
            '-- C '. join(' ', @$ra_cols_key ).' CNT_ALL '. join(' ', @a_no_key_cols ) ."\n" ;

      $ra_sql = get_sql_df_COL_CNT( $lda, $owner_1, $owner_2, $table_1, $table_2, $q1, $q2,
                                    $ra_cols_key, $ra_cols_expr_key, \@a_no_key_cols,
                                    $ra_Tab_cols, $rh_Col_ix ) ;
     }
   elsif( $fce == 3 ) # COLL diff details SQL
     {
      # set omitted coluns ($ra_cols_oth - omitted columns)
      if( scalar @$ra_cols_oth > 0 ) { cols_SET_tp_3( $ra_Tab_cols, $rh_Col_ix, $ra_cols_oth,'O') }
      # dif columns - if input is empty, use all testable (not key and not omitted) columns
      if( scalar @$ra_cols_dif == 0 )
        {
         # if $ra_cols_dif is empty => use all columns without key-cols and omitted cols
         my ( $ra_Col_prop, @a_cols_dif, $tp_3 ) ;
         for $ra_Col_prop ( @$ra_Tab_cols )
           {
            $tp_3 = $ra_Col_prop->[ 4 ] ;
            if( $tp_3 ne 'K' && $tp_3 ne 'O') { push( @a_cols_dif, $ra_Col_prop->[ 0 ] ) }
           }
         $ra_cols_dif = \@a_cols_dif ;
        }
      $ra_sql = get_sql_df_COL_VAL( $lda, $owner_1, $owner_2, $table_1, $table_2, $q1, $q2,
                                    $ra_cols_key, $ra_cols_expr_key, $ra_cols_dif,
                                    $ra_Tab_cols, $rh_Col_ix ) ;
     }
   else
     {
      print STDERR '-- ERROR: parameter fce='. $fce .' is unexpected.'."\n" ;
     }
   # print generated SQL
   if( defined $ra_sql && scalar @$ra_sql > 0 )
     {
      for $txt ( @$ra_sql ) { print $txt ."\n" }
      print '/'."\n\n" ;
     }
  }

1;
