# get_schema_ob.pl 
#
# extracts schema metadata (tables/views list and structure, PL/SQL code)
# into text files
#
# 2020-11-30: redesign (+added PL/SQL functions, procedures, types)

use integer ;
use strict ;
use warnings ;
use DBI ;
use Ora_LDA ;

# -- BEGIN --

$ENV{"NLS_SORT"} = 'BINARY' ;

my ( $Lda,         # Oracle session handle (logon data area)
     $No_cmts_flg, # no comments flg (true: exclude code comments, false: original code)
     $RE_name      # regular expression for OBJECT_NAME (optional)
   ) = get_cmd_line_param() ;

if( $Lda )
  {
   my $uid                 = Ora_LDA::get_uid( $Lda ) ;
   my ( $uname, $db_name ) = $uid =~ m/([\w]+)@([\w]+)/ ;
   my $subdir              = $db_name .'/'. $uname ;
   my $pref                = 'out' ;

   $Lda->{LongReadLen} = 1024 * 32 ;

   # create directories if doesn't exist
   if( ! -d $db_name ) { mkdir $db_name }
   if( ! -d $subdir )  { mkdir $subdir }

   # save parameters (append)
   save_label( $subdir, $pref, $db_name, $uname, $No_cmts_flg, $RE_name ) ;
   # CSV files:
   upload_obj( $subdir, $pref, 1 ) ;
   upload_col( $subdir, $pref, 2,'TABLE') ;
   upload_col( $subdir, $pref, 3,'VIEW') ;
   upload_inl( $subdir, $pref, 4 ) ;
   upload_inc( $subdir, $pref, 5 ) ;
   # SQL (views):
   upload_views( $subdir .'/sql') ;
   # PL/SQL:
   my $c_obj = $Lda->prepare('select a.OBJECT_NAME from USER_OBJECTS a
where a.OBJECT_TYPE = ? and regexp_like( a.OBJECT_NAME, ?)
order by a.OBJECT_NAME') ;
   my $c_src = $Lda->prepare('select a.TEXT from USER_SOURCE a where a.TYPE = ? and a.NAME = ? order by a.LINE') ;
   upload_pls( $c_obj, $c_src, $subdir .'/pls','PACKAGE') ;
   upload_pls( $c_obj, $c_src, $subdir .'/fce','FUNCTION') ;
   upload_pls( $c_obj, $c_src, $subdir .'/pro','PROCEDURE') ;
   upload_pls( $c_obj, $c_src, $subdir .'/trg','TRIGGER') ;
   upload_pls( $c_obj, $c_src, $subdir .'/typ','TYPE') ;

   $Lda->disconnect() or warn "Disconnect: $DBI::errstr\n\n" ;
  }

# -- END --

sub get_cmd_line_param
  {
   my ( $uid, $opt, $rgn ) = ('','','') ;
   my ( $arg, $lda, $no_cmts_flg ) ;

   if( scalar @ARGV > 0 )
     {
      for $arg ( @ARGV )
        {
         if(    ! $uid && $arg =~ /^\w+/ )        { $uid = $arg }
         elsif( ! $opt && index( $arg,'-') == 0 ) { $opt = lc($arg) }
         elsif( ! $rgn && index( $arg,'-') != 0 ) { $rgn = $arg }
         else
           { print STDERR 'Error, invalid arg.: '. $arg ."\n" }
        }

      if( ! $uid ) { print STDERR 'Error: no <userid>'."\n\n" }
     }

   if( $uid )
     {
      $lda = Ora_LDA::ora_LDA( $uid ) ;
      $no_cmts_flg = ( substr( $opt, 0, 3 ) eq '-nc') ? 1 : 0 ;
      if( ! $rgn ) { $rgn = '.*' }
     }
   else
     {
      print STDERR 'Utility for extract schema metadata (tables/views list and structure, PL/SQL code)'."\n".
                   'into text files (creates subdirectory for database name and for username)'."\n\n".
                   'Usage: get_schema_ob.pl <userid> [-cn] [<regexp_name>]'."\n".
                   '  <userid>      : username/password@db_name'."\n".
                   '  -nc           : no comments, views and PL/SQL code without code comments (optional)'."\n".
                   '  <regexp_name> : regular expresion for OBJECT_NAME (optional)'."\n\n" ;
     }

   return( $lda, $no_cmts_flg, $rgn ) ;
  }


sub get_filename_out
  {
   my ( $subdir, $pref, $seq, $id, $ext ) = @_ ;

   return $subdir .'/'. $pref .'_'. $seq .'_'. $id . $ext ;
  }


sub save_label
  {
   my ( $subdir, $pref, $db_name, $uname, $no_cmts_flg, $re_name ) = @_ ;
   my ( $sysdate, $uid, $fname, $exists_flg ) ;

   $sysdate = Ora_LDA::get_sysdate() ;
   $fname   = get_filename_out( $subdir, $pref, 0,'param','.txt') ;

   $exists_flg = ( -f $fname ) ? 1 : 0 ;

   open( OUT, ">>$fname") || die "Can't open file ". $fname ."\n\n" ;
   if( $exists_flg ) { print OUT "\n" }
   print OUT 'schema:      '. lc( $uname ) .'@'. uc( $db_name ) ."\n".
             'date:        '. $sysdate ."\n".
             'no_cmts_flg: '. (($no_cmts_flg) ? 'true (no comments)' : 'false (original source code)') ."\n" ;
   if( $re_name )
     {
      print OUT 're_name:     '. $re_name .' (regular expression for OBJECT_NAME)'."\n" ;
     }
   close( OUT ) ;

   print $fname .(( $exists_flg ) ? ' appended' : ' created.')."\n" ;
  }


sub rows_out
  {
   my ( $ra_rows, $fh, $obj_type ) = @_ ;
   my ( $line, $empty_line_flg ) ;

   $empty_line_flg = ($obj_type eq 'VIEW') ? 0 : 1 ;
   if( $No_cmts_flg ) # output without comments
     {
      my $code_flg = 1 ;         # TRUE  - enabled code (not commented)
      my $code_flg_change = 0 ;  # FALSE - change from enabled code to disabled code
      my ( $cmt_beg_pos, $cmt_end_pos, $cmt_flg ) ;

      foreach $line ( @$ra_rows )
        {
         # multirow comments /* ... */ remove:
         $cmt_flg = 0 ;
         while( $line )
           {
            $cmt_beg_pos = index( $line,'/*') ;
            $cmt_end_pos = index( $line,'*/') ;
            # print '>> '. $line .' ('. $cmt_beg_pos .', '. $cmt_end_pos ." )\n" ;
            if( $cmt_beg_pos == -1 && $cmt_end_pos == -1 ) { last }
            $cmt_flg = 1 ;

            if( $cmt_beg_pos >= 0 )
              {
               if( $cmt_end_pos == -1 )
                 { $line = substr( $line, 0, $cmt_beg_pos ) ; $code_flg_change = 1 ; next ; }
               elsif( $cmt_end_pos > $cmt_beg_pos )
                 { $line = substr( $line, 0, $cmt_beg_pos ) . substr( $line, $cmt_end_pos+2 ) ; next ; }
              }

            if( $cmt_end_pos >= 0 )
              {
               $line = substr( $line, $cmt_end_pos+2 ) ; $code_flg = 1 ;
              }
           }

         if( $code_flg )
           {
            $cmt_beg_pos = index( $line,'--') ;
            if( $cmt_beg_pos >= 0 ) { $line = substr( $line, 0, $cmt_beg_pos ) ; $cmt_flg = 1 ; }
            $line =~ s/\s+$// ;
            if( length( $line ) > 0 ) { print $fh $line ."\n" }
            elsif( $cmt_flg == 0 && $empty_line_flg ) { print $fh "\n" }

            if( $code_flg_change == 1 ) { $code_flg = $code_flg_change = 0 ; }
           }
        }
     }
   else # output with comments
     {
      foreach $line ( @$ra_rows )
        {
         $line =~ s/\s+$// ;
         if( $empty_line_flg || length( $line ) > 0 ) { print $fh $line ."\n" }
        }
     }
   print $fh "/\n" ;
  }


sub upload_views
  {
   my ( $subdir ) = @_ ;
   my ( $fh, $filename, $pathname ) ;
   my ( $c_vw, $view_name, $text ) ;
   my ( @Rows, $row, $first_flg ) ;

   $first_flg = 1 ; # true
   $c_vw = $Lda->prepare('select a.VIEW_NAME, a.TEXT
from USER_VIEWS a
where regexp_like( a.VIEW_NAME, ?)
order by a.VIEW_NAME') ;
   $c_vw->execute( $RE_name ) ;
   while( ( $view_name, $text ) = $c_vw->fetchrow_array() )
     {
      $filename = lc( $view_name ) .'_VW.sql' ;
      $pathname = $subdir .'/'. $filename ;
      if( $first_flg )
        {
         $first_flg = 0 ; if( ! -d $subdir ) { mkdir $subdir }
        }
      open( $fh,'>'.$pathname ) or die 'Error: '."can't open file ". $pathname .': '. $! ."\n\n" ;
      print $pathname .' created.'."\n" ;
      print $fh '-- '. $filename ."\n\n".
                'prompt >>> create view '. $view_name ."\n\n".
                'create or replace force view '. $view_name ."\nas\n" ;
      @Rows = split(/[\n]/, $text ) ;
      rows_out( \@Rows, $fh,'VIEW') ;
      @Rows = () ;
      close( $fh ) ;
     }
  }


sub upload_src
  {
   my ( $c_src, $fh, $obj_type, $obj_name ) = @_ ;
   my ( $line, $len, @Rows ) ;

   $c_src->execute( $obj_type, $obj_name ) ;
   ( $line ) = $c_src->fetchrow_array() ;
   if( $line )
     {
      chomp( $line ) ; $line =~ s/\s+$// ;
      print $fh "\n".'prompt >>> create '. lc($obj_type) .' '. $obj_name ."\n\n" ;
      $len = length($obj_type) ;
      if( lc( $obj_type ) eq lc( substr( $line, 0, $len ) ) )
        {
         $line = substr( $line, $len ) ; $line =~ s/^\s+// ;
         print $fh 'create or replace '. lc( $obj_type ) .' '. $line ."\n" ;
        }
      else
        { print $fh 'create or replace '. $line ."\n" }

      while( ( $line ) = $c_src->fetchrow_array() )
        {
         chomp( $line ) ; push( @Rows, $line ) ;
        }
      rows_out( \@Rows, $fh, $obj_type ) ;
      @Rows = () ;
     }
  }


sub upload_pls
  {
   my ( $c_obj, $c_src, $subdir, $pls_type ) = @_ ;
   my ( $fh, $filename, $pathname, $pls_name, $first_flg ) ;

   $first_flg = 1 ; # true
   $c_obj->execute( $pls_type, $RE_name ) ;
   while( ( $pls_name ) = $c_obj->fetchrow_array() )
     {
      $filename = lc( $pls_name ) .'.pls' ;
      $pathname = $subdir .'/'. $filename ;
      if( $first_flg )
        {
         $first_flg = 0 ; if( ! -d $subdir ) { mkdir $subdir }
        }
      open( $fh,'>'.$pathname ) or die 'Error: '."can't open file ". $pathname .': '. $! ."\n\n" ;
      print $pathname .' created.'."\n" ;

      print $fh '-- '. $filename ."\n" ;
      upload_src( $c_src, $fh, $pls_type, $pls_name ) ;
      if( $pls_type eq 'PACKAGE')
        {
         upload_src( $c_src, $fh,'PACKAGE BODY', $pls_name ) ;
        }
      close( $fh ) ;
     }
  }


sub upload_col
  {
   my ( $subdir, $pref, $seq, $object_type ) = @_ ;
   my ( $fh, $filename, $pathname ) ;
   my ( $c_tab, $c_col, $tab_name ) ;
   my ( $col_id, $col_name, $data_type, $nullable ) ;

   $filename = get_filename_out( $subdir, $pref, $seq, lc( substr( $object_type, 0, 3 )), '.csv') ;
   open( $fh,'>'.$filename ) or die 'Error: '."can't open file ". $filename .': '. $! ."\n\n" ;
   print $filename .' created.'."\n" ;
   print $fh '#'. lc( $object_type ) .'_columns#'."\n" ;

   $c_tab = $Lda->prepare('select a.OBJECT_NAME
from USER_OBJECTS a
where a.OBJECT_TYPE = ? and regexp_like( a.OBJECT_NAME, ?)
order by a.OBJECT_NAME') ;
   $c_col = $Lda->prepare("\
select
  a.COLUMN_ID,
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
    else
      a.DATA_TYPE
  end as DATA_TP,
  a.NULLABLE
from
  USER_TAB_COLUMNS a
where
  a.TABLE_NAME = ?
order by
  a.COLUMN_NAME") ;

   $c_tab->execute( $object_type, $RE_name ) ;
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
   my ( $subdir, $pref, $seq, $object_type ) = @_ ;
   my ( $fh, $filename, $pathname ) ;
   my ( $c_inl, @Cols ) ;

   $filename = get_filename_out( $subdir, $pref, $seq,'inl','.csv') ;
   open( $fh,'>'.$filename ) or die 'Error: '."can't open file ". $filename .': '. $! ."\n\n" ;
   print $filename .' created.'."\n" ;
   print $fh '#indexes#'."\n" ;

   $c_inl = $Lda->prepare('select a.TABLE_NAME, a.INDEX_NAME, a.INDEX_TYPE, a.UNIQUENESS, a.STATUS, a.PARTITIONED
from USER_INDEXES a
where regexp_like( a.TABLE_NAME,?)
order by a.TABLE_NAME, a.INDEX_NAME') ;

   print $fh 'TABLE_NAME;INDEX_NAME;INDEX_TYPE;UNIQUENESS;STATUS;PARTITIONED',"\n" ;
   $c_inl->execute( $RE_name ) ;
   while( @Cols = $c_inl->fetchrow_array() )
     {
      print $fh join(';', @Cols ) ."\n" ;
     }
   close( $fh ) ;
  }


sub upload_inc
  {
   my ( $subdir, $pref, $seq, $object_type ) = @_ ;
   my ( $fh, $filename, $pathname ) ;
   my ( $c_inc, @Cols ) ;

   $filename = get_filename_out( $subdir, $pref, $seq, 'inc','.csv') ;
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
where
  regexp_like( a.TABLE_NAME, ? )
order by
  a.TABLE_NAME, a.INDEX_NAME, a.COLUMN_POSITION") ;

   print $fh 'TABLE_NAME;INDEX_NAME;COLUMN_POSITION;COLUMN_NAME;NULLABLE'."\n" ;
   $c_inc->execute( $RE_name ) ;
   while( @Cols = $c_inc->fetchrow_array() )
     {
      print $fh join(';', @Cols ) ."\n" ;
     }
   close( $fh ) ;
  }


sub upload_obj
  {
   my ( $subdir, $pref, $seq ) = @_ ;
   my ( $fh, $filename, $pathname ) ;
   my ( $c_inc, @Cols ) ;

   $filename = get_filename_out( $subdir, $pref, $seq,'obj','.csv') ;
   open( $fh,'>'.$filename ) or die 'Error: '."can't open file ". $filename .': '. $! ."\n\n" ;
   print $filename .' created.'."\n" ;
   print $fh '#database_objects#'."\n" ;

   $c_inc = $Lda->prepare("\
select a.OBJECT_TYPE, a.OBJECT_NAME, a.STATUS
from
  USER_OBJECTS a
where
  a.OBJECT_TYPE not in ('INDEX PARTITION',
                        'TABLE PARTITION')
  and regexp_like( a.OBJECT_NAME, ? )
order by
  a.OBJECT_TYPE, a.OBJECT_NAME") ;

   print $fh 'OBJECT_TYPE;OBJECT_NAME;STATUS'."\n" ;
   $c_inc->execute( $RE_name ) ;
   while( @Cols = $c_inc->fetchrow_array() )
     {
      print $fh join(';', @Cols ) ."\n" ;
     }
   close( $fh ) ;
  }
