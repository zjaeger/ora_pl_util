#!/usr/bin/perl
# get_schema_ob.pl
#
# extracts schema metadata

use integer ;
use strict ;
use warnings ;
use DBI ;
use Ora_LDA ;

# -- BEGIN --

$ENV{"NLS_SORT"} = 'BINARY' ;

unless( $ARGV[ 0 ] ) { die "No args ( <connect_string> [ view_name ] ).\n\n" ; }

my $Lda = Ora_LDA::ora_LDA( $ARGV[ 0 ] ) ;

if( $Lda )
  {
   my $uid                 = Ora_LDA::get_uid( $Lda ) ;
   my ( $uname, $db_name ) = $uid =~ m/([\w]+)@([\w]+)/ ;

   if( ! -d $db_name )         { mkdir $db_name }
   if( ! -d $db_name .'/pls' ) { mkdir $db_name .'/pls' }
   if( ! -d $db_name .'/trg' ) { mkdir $db_name .'/trg' }
   if( ! -d $db_name .'/sql' ) { mkdir $db_name .'/sql' }

   $Lda->{LongReadLen} = 16384 ;

   save_label( $uname, $db_name ) ;
   # SQL:
   upload_views( $db_name ) ;
   # PLS:
   upload_pkg( $db_name ) ;
   upload_trg( $db_name ) ;
   # CSV:
   upload_col(   $uname, $db_name, 1,'TABLE') ;
   upload_col(   $uname, $db_name, 2,'VIEW') ;
   upload_inl(   $uname, $db_name, 3 ) ;
   upload_inc(   $uname, $db_name, 4 ) ;
   upload_oth(   $uname, $db_name, 5 ) ;

   $Lda->disconnect() or warn "Disconnect: $DBI::errstr\n\n" ;
  }

# -- END --

sub get_filename_out
  {
   my ( $db_name, $uname, $seq, $id, $ext ) = @_ ;

   return $db_name .'/'. $uname .'_'. $seq .'_'. $id . $ext ;
  }


sub get_pathname_out
  {
   my ( $db_name, $subdir, $filename  ) = @_ ;

   return $db_name .'/'. $subdir .'/'. $filename ;
  }

sub save_label
  {
   my ( $uname, $db_name ) = @_ ;
   my ( $sysdate, $uid, $fname ) ;

   $sysdate = Ora_LDA::get_sysdate() ;
   $fname   = get_filename_out( $db_name, $uname, 0,'timestamp','.txt') ;

   open( OUT, ">$fname") || die "Can't open file ". $fname ."\n\n" ;
   print OUT 'Date:   '. $sysdate ."\n".
             'Schena: '. lc( $uname ) .'@'. uc( $db_name ) ."\n" ;
   close( OUT ) ;

   print 'File '. $fname .' created.'."\n" ;
  }


sub upload_views
  {
   my ( $db_name ) = @_ ;
   my ( $fh, $filename, $pathname ) ;
   my ( $c_vw, $view_name, $text ) ;
   my ( @Rows, $row ) ;

   $c_vw = $Lda->prepare('select VIEW_NAME, TEXT from USER_VIEWS order by VIEW_NAME') ;
   $c_vw->execute() ;
   while( ( $view_name, $text ) = $c_vw->fetchrow_array() )
     {
      $filename = lc( $view_name ) .'_VW.sql' ;
      $pathname = get_pathname_out( $db_name,'sql', $filename ) ;
      open( $fh,'>'.$pathname ) or die 'Error: '."can't open file ". $pathname .': '. $! ."\n\n" ;
      print $pathname .' created.'."\n" ;
      print $fh '-- '. $filename ."\n\n".
                'create or replace force view '. $view_name ."\nas\n" ;
      @Rows = split(/[\n]/, $text ) ;
      foreach $row ( @Rows )
        {
         $row =~ s/[\s]+$// ;
         if( length( $row ) > 0 ) { print $fh $row ."\n" }
        }
      print $fh "/\n\n" ;
      close( $fh ) ;
     }
  }


sub upload_pkg
  {
   my ( $db_name ) = @_ ;
   my ( $fh, $filename, $pathname ) ;
   my ( $c_pkg, $c_src, $pkg_name, $text, $first_flg ) ;

   $c_pkg = $Lda->prepare('select OBJECT_NAME from USER_OBJECTS where OBJECT_TYPE = ? order by OBJECT_NAME') ;
   $c_src = $Lda->prepare('select rtrim( TEXT ) from USER_SOURCE where TYPE = ? and NAME = ? order by LINE') ;

   $c_pkg->execute('PACKAGE') ;
   while( ( $pkg_name ) = $c_pkg->fetchrow_array() )
     {
      $filename = lc( $pkg_name ) .'.pls' ;
      $pathname = get_pathname_out( $db_name,'pls', $filename ) ;
      open( $fh,'>'.$pathname ) or die 'Error: '."can't open file ". $pathname .': '. $! ."\n\n" ;
      print $pathname .' created.'."\n" ;

      print $fh '-- '. $filename ."\n\n".
                'create or replace ' ;
      $c_src->execute('PACKAGE', $pkg_name ) ;
      while( ( $text ) = $c_src->fetchrow_array() ) { if( $text ) { print $fh $text }}
      print $fh "\n/\n\n" ;

      $first_flg = 1 ;
      $c_src->execute('PACKAGE BODY', $pkg_name ) ;
      while( ( $text ) = $c_src->fetchrow_array() )
        {
         if( $first_flg ) { $first_flg = 0 ; print $fh 'create or replace ' ; }
         print $fh $text ;
        }
      if( $first_flg != 1 ) { print $fh "\n/\n" }
      close( $fh ) ;
     }
  }


sub upload_trg
  {
   my ( $db_name ) = @_ ;
   my ( $fh, $filename, $pathname ) ;
   my ( $c_trg, $c_src, $trg_name, $text ) ;

   $c_trg = $Lda->prepare('select OBJECT_NAME from USER_OBJECTS where OBJECT_TYPE = ? order by OBJECT_NAME') ;
   $c_src = $Lda->prepare('select rtrim( TEXT ) from USER_SOURCE where TYPE = ? and NAME = ? order by LINE') ;

   $c_trg->execute('TRIGGER') ;
   while( ( $trg_name ) = $c_trg->fetchrow_array() )
     {
      $filename = lc( $trg_name ) .'.pls' ;
      $pathname = get_pathname_out( $db_name,'trg', $filename ) ;
      open( $fh,'>'.$pathname ) or die 'Error: '."can't open file ". $pathname .': '. $! ."\n\n" ;
      print $pathname .' created.'."\n" ;

      print $fh '-- '. $filename ."\n\n".
                'create or replace ' ;
      $c_src->execute('TRIGGER', $trg_name ) ;
      while( ( $text ) = $c_src->fetchrow_array() ) { if( $text ) { print $fh $text }}
      print $fh "\n/\n" ;
      close( $fh ) ;
     }
  }


sub upload_col
  {
   my ( $uname, $db_name, $seq, $object_type ) = @_ ;
   my ( $fh, $filename, $pathname ) ;
   my ( $c_tab, $c_col, $tab_name ) ;
   my ( $col_id, $col_name, $data_type, $nullable ) ;

   $filename = get_filename_out( $db_name, $uname, $seq, lc( substr( $object_type, 0, 3 )), '.csv') ;
   open( $fh,'>'.$filename ) or die 'Error: '."can't open file ". $filename .': '. $! ."\n\n" ;
   print $filename .' created.'."\n" ;
   print $fh '#'. lc( $object_type ) .'_columns#'."\n" ;

   $c_tab = $Lda->prepare('select OBJECT_NAME from USER_OBJECTS where OBJECT_TYPE = ? order by OBJECT_NAME') ;
   $c_col = $Lda->prepare("\
SELECT
  A.column_id,
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
    ELSE
      A.data_type
  END AS data_tp,
  A.nullable
FROM
  user_tab_columns A
WHERE
  A.table_name = ?
ORDER BY
  A.column_name") ;

   $c_tab->execute( $object_type ) ;
   while( ( $tab_name ) = $c_tab->fetchrow_array() )
     {
      $c_col->execute( $tab_name ) ;
      print $fh "\n". $tab_name .';'. $object_type ."\n" ;
      while( ( $col_id, $col_name, $data_type, $nullable ) = $c_col->fetchrow_array() )
        {
         print $fh join(';', ( $tab_name, $col_name, $data_type, $nullable )) ."\n" ;
        }
     }
   close( $fh ) ;
  }


sub upload_inl
  {
   my ( $uname, $db_name, $seq ) = @_ ;
   my ( $fh, $filename, $pathname ) ;
   my ( $c_inl, @Cols ) ;

   $filename = get_filename_out( $db_name, $uname, $seq,'inl','.csv') ;
   open( $fh,'>'.$filename ) or die 'Error: '."can't open file ". $filename .': '. $! ."\n\n" ;
   print $filename .' created.'."\n" ;
   print $fh '#indexes#'."\n" ;

   $c_inl = $Lda->prepare('select TABLE_NAME, INDEX_NAME, INDEX_TYPE, UNIQUENESS, STATUS, PARTITIONED
from USER_INDEXES order by TABLE_NAME, INDEX_NAME') ;

   print $fh 'TABLE_NAME;INDEX_NAME;INDEX_TYPE;UNIQUENESS;STATUS;PARTITIONED',"\n" ;
   $c_inl->execute() ;
   while( @Cols = $c_inl->fetchrow_array() )
     {
      print $fh join(';', @Cols ) ."\n" ;
     }
   close( $fh ) ;
  }


sub upload_inc
  {
   my ( $uname, $db_name, $seq ) = @_ ;
   my ( $fh, $filename, $pathname ) ;
   my ( $c_inc, @Cols ) ;

   $filename = get_filename_out( $db_name, $uname, $seq, 'inc','.csv') ;
   open( $fh,'>'.$filename ) or die 'Error: '."can't open file ". $filename .': '. $! ."\n\n" ;
   print $filename .' created.'."\n" ;
   print $fh '#index_columns#'."\n" ;

   $c_inc = $Lda->prepare("\
select
  a.TABLE_NAME, a.INDEX_NAME, a.COLUMN_POSITION, a.COLUMN_NAME, b.NULLABLE
from
  USER_IND_COLUMNS a
  inner join USER_TAB_COLUMNS b on ( a.TABLE_NAME = b.TABLE_NAME
                                     and a.COLUMN_NAME = b.COLUMN_NAME )
order by
  a.TABLE_NAME, a.INDEX_NAME, a.COLUMN_POSITION") ;

   print $fh 'TABLE_NAME;INDEX_NAME;COLUMN_POSITION;COLUMN_NAME;NULLABLE'."\n" ;
   $c_inc->execute() ;
   while( @Cols = $c_inc->fetchrow_array() )
     {
      print $fh join(';', @Cols ) ."\n" ;
     }
   close( $fh ) ;
  }


sub upload_oth
  {
   my ( $uname, $db_name, $seq ) = @_ ;
   my ( $fh, $filename, $pathname ) ;
   my ( $c_inc, @Cols ) ;

   $filename = get_filename_out( $db_name, $uname, $seq, 'oth','.csv') ;
   open( $fh,'>'.$filename ) or die 'Error: '."can't open file ". $filename .': '. $! ."\n\n" ;
   print $filename .' created.'."\n" ;
   print $fh '#database_objects#'."\n" ;

   $c_inc = $Lda->prepare("\
select OBJECT_TYPE, OBJECT_NAME, STATUS
from   user_objects
where  OBJECT_TYPE not in (
'INDEX',
'INDEX PARTITION',
'TABLE',
'TABLE PARTITION',
'VIEW')
order by OBJECT_TYPE, OBJECT_NAME") ;

   print $fh 'OBJECT_TYPE;OBJECT_NAME;STATUS'."\n" ;
   $c_inc->execute() ;
   while( @Cols = $c_inc->fetchrow_array() )
     {
      print $fh join(';', @Cols ) ."\n" ;
     }
   close( $fh ) ;
  }


