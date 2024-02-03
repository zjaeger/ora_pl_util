# data_io.pl
#
# Data in/out:
# - copy from (userid_1:) input query (source table) to (userid_2:) target table
# - export from query to output datafile
# - import from datafile to target table
#
# Parameters: <userid_1> [<userid_2>] [-s(ingle)] [-v(erbose)] <cmdfile>
#
# -s - single row insert (default: bulk insert)
#
# cmdfile content:
# insert [into] <(userid_2:)table_name> [using] <(userid_1:)select_command>
# /
# export [into] <file_name> [using] <select_command>
# /
# import <datafile> into <table_name>
# /
#
# 2022-10-12

use strict ;
use warnings ;
use integer ;
use DBI ;
use Ora_LDA ;

# --- BEGIN

my ( $userid_1,   # source connect string
     $userid_2,   # target connect string
     $cmdfile,    # cmd file name
     $single_flg, # 0: bulk insert, 1: singe row insert
     $verb_flg    # verbose flag
   ) = get_input_params() ;

my $Lda_1 = Ora_LDA::ora_LDA( $userid_1 ) ;    # source schema session
my $Lda_2 ;

if( $Lda_1 )
  {
   print '>> connect '. Ora_LDA::get_uid( $Lda_1 ) ."\n" ;
   set_NLS( $Lda_1, 1 ) ;
   $Lda_1->{RowCacheSize} = 4096 ;

   if( $userid_2 )
     {
      $Lda_2 = Ora_LDA::ora_LDA( $userid_2 ) ; # target schema session

      if( $Lda_2 )
        {
         print "\n".'>> connect '. Ora_LDA::get_uid( $Lda_2 ) .' (target schema)'."\n" ;
         set_NLS( $Lda_2, 1 ) ;
        }
     }
   print "\n" ;

   if( ! $userid_2 || $Lda_2 )
     {
      my $tm_start = time() ;

      # process cmd file (insert/export/import commands)
      process_cmdfile( $Lda_1, $Lda_2, $cmdfile, $single_flg, $verb_flg ) ;

      print 'Elapsed time: '. (time() - $tm_start) ."\n" ;

      if( $Lda_2 ) { $Lda_2->disconnect() or warn "Disconnect: $DBI::errstr\n\n" }
     }
   $Lda_1->disconnect() or warn "Disconnect: $DBI::errstr\n\n" ;
  }

# --- END

sub basename
  {
   my ( $pathname ) = @_ ;
   my ( $pos1, $pos2, $pos ) ;

   $pos1 = rindex( $pathname,'\\') ;
   $pos2 = rindex( $pathname,'/') ;

   $pos = ( $pos1 > $pos2 ) ? $pos1 : $pos2 ;

   if( $pos > -1 ) { return substr( $pathname, $pos+1 ) }
   else            { return $pathname }
  }


sub get_input_params
  {
   my ( $userid_1,  # connect string
        $userid_2,  # connect string
        $cmdfile,
        $single_flg,
        $verb_flg
      ) ;
   my $val ;

   foreach $val ( @ARGV )
     {
      if( $val =~ /^[\w]+\/[\S]+@[\w\.]+$/
          || ( $val =~ /^\w+$/ && exists $ENV{ $val } ) )
        {
         if(    ! defined( $userid_1 )) { $userid_1 = $val }
         elsif( ! defined( $userid_2 )) { $userid_2 = $val }
         else
           { print STDERR 'Unexpected value (userid): '. $val ."\n" }
        }
      elsif( -f $val )
        {
         if( ! defined( $cmdfile )) { $cmdfile = $val }
         else
           { print STDERR 'Unexpected value: '. $val ."\n" }
        }
      elsif( $val =~ /^-s/i )
        {
         if( ! defined( $single_flg )) { $single_flg = 1 }
         else
           { print STDERR 'Unexpected value: '. $val ."\n" }
        }
      elsif( $val =~ /^-v/i )
        {
         if( ! defined( $verb_flg )) { $verb_flg = 1 }
         else
           { print STDERR 'Unexpected value: '. $val ."\n" }
        }
      else
        { print STDERR 'Unexpected value: '. $val ."\n" }
     }

   unless(    defined( $userid_1 )
           && defined( $cmdfile ) )
     {
      if( scalar @ARGV > 0 ) { print STDERR 'Invalid parameters'."\n" }
      printf STDERR 'Usage: '. basename( $0 ) .q{ <userid_1> [<userid_2>] [-s(ingle)] [-v(erbose)] <cmdfile>
-s - single row insert (default: bulk insert)
cmdfile content:
insert [into] <(userid_2:)table_name> [using] <(userid_1:)select_command>
/
export [into] <file_name> [using] <select_command>
/
import <file_name> into <table_name>
/
} ;
      exit 1 ;
     }

   if( ! defined( $userid_2 ))   { $userid_2 = '' }
   if( ! defined( $single_flg )) { $single_flg = 0 }
   if( ! defined( $verb_flg ))   { $verb_flg = 0 }

   return ( $userid_1, $userid_2, $cmdfile, $single_flg, $verb_flg ) ;
  }


sub set_NLS
  {
   my ( $lda, $print_flg ) = @_ ;
   my ( $date_mask, $c_alter, $cmd ) ;

   $date_mask = 'YYYYMMDDHH24MISS' ;
   $cmd = q{alter session set NLS_DATE_FORMAT='} . $date_mask . q{'} ;

   $c_alter = $lda->prepare( $cmd ) ;
   $c_alter->execute() ;
   if( $print_flg ) { print '>> '. $cmd ."\n" }

   $cmd = q{alter session set NLS_NUMERIC_CHARACTERS='.,'} ;

   $c_alter = $lda->prepare( $cmd ) ;
   $c_alter->execute() ;
   if( $print_flg ) { print '>> '. $cmd ."\n" }
  }


sub get_COLS
  {
   my ( $lda, $tab_name ) = @_ ;
   my ( $ra_set, $ra_row, @a_col ) ;
   my $c_col = $lda->prepare( q{
select a.COLUMN_NAME
from   USER_TAB_COLUMNS a
where  a.TABLE_NAME = ?
order by a.COLUMN_ID
} ) ;

   $c_col->execute( $tab_name ) ;
   $ra_set = $c_col->fetchall_arrayref() ;

   @a_col = () ;
   for $ra_row ( @$ra_set ) { push( @a_col, $ra_row->[0] ) }

   return \@a_col ;
  }


sub is_table_empty
  {
   my ( $lda, $tab_name ) = @_ ;
   my $cnt = $lda->selectrow_array('select count(*) as CNT from '. $tab_name .' where ROWNUM < 5') ;

   return ($cnt == 0) ? 1 : 0 ;
  }


sub get_INSERT
  {
   my ( $tab_name, $ra_col ) = @_ ;
   my ( $col_name, $sql_text, $delim, $first_flg ) ;

   $sql_text = 'insert /*+append*/ into '. $tab_name .'(' ;
   $delim = "\n  " ;
   $first_flg = 1 ;
   for $col_name ( @$ra_col )
     {
      $sql_text .= $delim . $col_name ;
      if( $first_flg ) { $first_flg = 0 ; $delim = ",\n  " }
     }
   $sql_text .= " )\n".
                'values('."\n".
                '  ?'. (', ?' x ((scalar @$ra_col) -1) ) .' )' ;

   return \$sql_text ;
  }


sub tab_copy_SINGLE
  {
   my ( $c1, $c2 ) = @_ ;
   my ( $ra_set, $ra_row, $rows ) ;

   $c1->execute() ;

   for( $rows = 0 ; ; )
     {
      print STDERR '.' ;
      $ra_set = $c1->fetchall_arrayref( undef, (16*1024) ) ;
      if( defined $ra_set && scalar @$ra_set > 0 )
        {
         $rows += scalar @$ra_set ;
         for $ra_row ( @$ra_set ) { $c2->execute( @$ra_row ) }
        }
      else
        { last }
     }

   return $rows ;
  }


sub tab_copy_BULK
  {
   my ( $c1, $c2, $cols ) = @_ ;
   my ( $ra_row, $ix, @a_cols, $rows, $rows_all ) ;

   $c1->execute() ;

   while( $ra_row = $c1->fetchrow_arrayref() )
     {
      for( $ix = 0 ; $ix < $cols ; ++$ix ) { push( @{$a_cols[$ix]}, $ra_row->[$ix ] ) }

      if( ++$rows >= (1024*8) )
        {
         print STDERR '.' ;
         for( $ix = 0 ; $ix < $cols ; ++$ix ) { $c2->bind_param_array( $ix +1, $a_cols[ $ix ] ) }
         $c2->execute_array( {} ) ;
         for( $ix = 0 ; $ix < $cols ; ++$ix ) { $a_cols[$ix] = [] }
         $rows_all += $rows ; $rows = 0 ;
        }
     }

   if( $rows > 0 )
     {
      for( $ix = 0 ; $ix < $cols ; ++$ix ) { $c2->bind_param_array( $ix +1, $a_cols[ $ix ] ) }
      $c2->execute_array( {} ) ;
      $rows_all += $rows ;
     }

   return $rows_all ;
  }


sub tab_copy
  {
   my ( $lda_1, $lda_2, $single_flg, $tab_name, $r_txt_select, $verb_flg ) = @_ ;
   my ( @a_col_1, $ra_col_2, %h_col_2, $cnt_err, $r_txt_insert ) ;
   my ( $c1, $c2, $rows, $tm_start ) ;

   $ra_col_2 = get_COLS( $lda_2, $tab_name ) ;
   if( scalar @$ra_col_2 == 0 )
     {
      print STDERR 'Error: invalid target table ('. $tab_name .').'."\n" ;
     }
   else
     {
      if( is_table_empty( $lda_2, $tab_name ) != 1 )
        {
         print STDERR 'Warning: target table '. $tab_name .' is not empty.'."\n" ;
        }
      print '>> insert into '. $tab_name ."\n" ;
      if( $verb_flg ) { print $$r_txt_select ."\n/\n" }

      $c1 = $lda_1->prepare( $$r_txt_select,
                             { ora_prefetch_memory => (1024*1024*8)
                             # ora_exe_mode        => OCI_STMT_SCROLLABLE_READONLY
                             } ) ;

      %h_col_2 = map { $_ => 0 } @$ra_col_2 ;
      ( @a_col_1 ) = @{$c1->{NAME_uc}} ;

      $cnt_err = 0 ;
      for my $col_name ( @a_col_1 )
        {
         if( ! exists $h_col_2{ $col_name } )
           {
            ++$cnt_err ;
            print STDERR 'Error: column does not exists: '. $tab_name .'.'. lc( $col_name ) .".\n" ;
           }
        }

      if( $cnt_err == 0 )
        {
         $r_txt_insert = get_INSERT( $tab_name, \@a_col_1 ) ;
         $c2 = $lda_2->prepare( $$r_txt_insert ) ;

         $tm_start = time() ;

         eval
           {
            if( $single_flg )
              { $rows = tab_copy_SINGLE( $c1, $c2 ) }
            else
              { $rows = tab_copy_BULK( $c1, $c2, scalar @a_col_1 ) }
           } ;

         if( $@ ) { $lda_2->rollback() }
         else
           {
            print "\n". $rows .' rows inserted, time: '. (time() - $tm_start) ."\n\n" ;
            $lda_2->commit() ;
           }
        }
     }
  }


sub tab_exp
  {
   my ( $lda, $file_name, $r_txt_select, $verb_flg ) = @_ ;
   my ( $out, $c1, $delim, $ra_val, $tm_start ) ;

   print '>> export into '. $file_name ."\n" ;
   $delim = '|' ;
   if( ! open( $out,'>', $file_name )) { print STDERR 'Error on open(>'. $file_name .'): '. $! ."\n" }
   else
     {
      if( $verb_flg ) { print $$r_txt_select ."\n/\n" }

      $c1 = $lda->prepare( $$r_txt_select,
                           { ora_prefetch_memory => (1024*1024*8)
                           # ora_exe_mode        => OCI_STMT_SCROLLABLE_READONLY
                           } ) ;
      # header
      print $out join( $delim, @{$c1->{NAME_uc}} ) ."\n" ;
      $tm_start = time() ;

      no warnings 'uninitialized';

      $c1->execute() ;
      while( $ra_val = $c1->fetchrow_arrayref() ) { print $out join( $delim, @$ra_val ) ."\n" }

      use warnings 'uninitialized';

      print $c1->rows .' rows saved, time: '. (time() - $tm_start) ."\n\n" ;

      close $out ;
     }
  }


sub tab_imp_SINGLE
  {
   my ( $c_ins, $in, $re_delim ) = @_ ;
   my ( @a_val, $rows ) ;

   $rows = 0 ;
   while( my $row = <$in> )
     {
      chomp( $row ) ;
      @a_val = split( $re_delim, $row ) ;

      $c_ins->execute( @a_val ) ;
      ++$rows ;
     }

   return $rows ;
  }


sub tab_imp_BULK
  {
   my ( $c_ins, $in, $re_delim, $cols ) = @_ ;
   my ( $ix, @a_val, @a_cols, $rows, $rows_all ) ;

   $rows = 0 ;
   while( my $row = <$in> )
     {
      chomp( $row ) ;
      @a_val = split( $re_delim, $row ) ;

      for( $ix = 0 ; $ix < $cols ; ++$ix ) { push( @{$a_cols[$ix]}, $a_val[$ix ] ) }

      if( ++$rows >= (1024*8) )
        {
         print STDERR '.' ;
         for( $ix = 0 ; $ix < $cols ; ++$ix ) { $c_ins->bind_param_array( $ix +1, $a_cols[ $ix ] ) }
         $c_ins->execute_array( {} ) ;
         for( $ix = 0 ; $ix < $cols ; ++$ix ) { $a_cols[$ix] = [] }
         $rows_all += $rows ; $rows = 0 ;
        }
     }

   if( $rows > 0 )
     {
      for( $ix = 0 ; $ix < $cols ; ++$ix ) { $c_ins->bind_param_array( $ix +1, $a_cols[ $ix ] ) }
      $c_ins->execute_array( {} ) ;
      $rows_all += $rows ;
     }

   return $rows_all ;
  }


sub tab_imp
  {
   my ( $lda, $datafile, $tab_name, $delim, $single_flg, $verb_flg ) = @_ ;
   my ( $in ) ;

   my $ra_col_2 = get_COLS( $lda, $tab_name ) ;

   if( scalar @$ra_col_2 == 0 )
     {
      print STDERR 'Error: invalid target table ('. $tab_name .').'."\n" ;
     }
   else
     {
      if( is_table_empty( $lda, $tab_name ) != 1 )
        {
         print STDERR 'Warning: target table '. $tab_name .' is not empty.'."\n" ;
        }

      if( ! open( $in, $datafile )) { print STDERR 'Error on open('. $datafile .'): '. $! ."\n" }
      else
        {
         my $cnt_err  = 0 ;
         my %h_col_2  = map { $_ => 0 } @$ra_col_2 ;
         my $val      = "\\". $delim ;
         my $re_delim = qr/$val/ ;
         my ( $row, @a_col ) ;

         if( defined( $row = <$in>) )
           {
            chomp( $row ) ;
            @a_col = split( $re_delim, $row ) ;

            for my $col_name ( @a_col )
              {
               if( ! exists $h_col_2{ $col_name } )
                 {
                  ++$cnt_err ;
                  print STDERR 'Error: column does not exists: '. $tab_name .'.'. lc( $col_name ) .".\n" ;
                 }
              }

            if( $cnt_err == 0 )
              {
               my $r_txt_insert = get_INSERT( $tab_name, \@a_col ) ;
               my $c_ins        = $lda->prepare( $$r_txt_insert ) ;
               my $tm_start     = time() ;
               my $rows ;

               print '>> load from '. $datafile .' into '. $tab_name ."\n" ;

               if( $verb_flg ) { print $$r_txt_insert ."\n/\n" }
               eval
                 {
                  if( $single_flg )
                    { $rows = tab_imp_SINGLE( $c_ins, $in, $re_delim ) }
                  else
                    { $rows = tab_imp_BULK(   $c_ins, $in, $re_delim, scalar @a_col ) }
                 } ;

               if( $@ ) { $lda->rollback() }
               else
                 {
                  print "\n". $rows .' rows inserted, time: '. (time() - $tm_start) ."\n\n" ;
                  $lda->commit() ;
                 }
              }
           }
         close( $in ) ;
        }
     }
  }


sub process_cmdfile
  {
   my ( $lda_1, $lda_2, $cmdfile, $single_flg, $verb_flg ) = @_ ;
   my ( $in, $row,
        $cmd_flg,   # 0: unknown, 1: INSERT or EXPORT, 2: SELECT cmd for INSERT or EXPORT, 3: IMPORT
        $cmd_tp,    # I: insert, E: export, L: load (import)
        $cmd_seq,   # command sequence number
        $obj_name,  # source table name OR target file name
        $datafile   # import input file name
      ) ;
   my ( @a_val, $uc_val, @a_sql, $sql_txt ) ;

   if( ! open( $in, $cmdfile )) { print STDERR 'Error on open('. $cmdfile .'): '. $! ."\n" }
     {
      $cmd_flg = $cmd_seq = 0 ;
      while( $row = <$in> )
        {
         if( $row =~ /^--\s+/ ) { next }
         chomp( $row ) ; $row =~ s/\s+$// ;

         if( $cmd_flg == 2 ) # INSERT or EXPORT command: SELECT command reading
           {
            if( substr( $row, -1, 1 ) eq ';')
              {
               $row =~ s/[;\s]$// ; push( @a_sql, $row ) ; $row = '/' ;
              }

            if( substr( $row,0,1) eq '/')
              {
               $sql_txt = join("\n", @a_sql ) ; @a_sql = () ;

               if( $cmd_tp eq 'I')
                 {
                  if( ! $obj_name ) { print STDERR 'Error ['. $cmd_seq .']: source table undefined.'."\n" ; last ; }
                  elsif( ! $lda_2 ) { print STDERR 'Error ['. $cmd_seq .']: no target connection string (<userid_2 is null).'."\n" ; last ; }
                  else              { tab_copy( $lda_1, $lda_2, $single_flg, $obj_name, \$sql_txt, $verb_flg ) }
                 }
               elsif( $cmd_tp eq 'E')
                 {
                  if( ! $obj_name ) { print STDERR 'Error ['. $cmd_seq .']: target file name undefined.'."\n" ; last ; }
                  else              { tab_exp( $lda_1, $obj_name, \$sql_txt, $verb_flg ) }
                 }
               else
                 { print STDERR 'Error ['. $cmd_seq .']: invalid cmd_type ('. ((defined $cmd_tp) ? $cmd_tp : '-') .").\n" ; last ; }

               $cmd_flg = 0 ; $obj_name = undef ;
              }
            else
              { push( @a_sql, $row ) }
           }
         else
           {
            @a_val = split(/\s+/, $row ) ; # split row into words by blanks
            for my $val ( @a_val )
              {
               $uc_val = uc( $val ) ;
               if( $cmd_flg == 0 ) # look for command
                 {
                  if(    $uc_val eq 'INSERT' || $uc_val eq 'EXPORT') { $cmd_flg = 1 ; ++$cmd_seq ; $cmd_tp = substr( $uc_val,0,1 ) ; }
                  elsif( $uc_val eq 'IMPORT' || $uc_val eq 'LOAD' )  { $cmd_flg = 3 ; ++$cmd_seq ; $cmd_tp = 'L' ; } # 'L': 'LOAD'
                  else
                    {
                     $cmd_tp = undef ;
                     if( $val =~ /\w+/ )
                       { warn 'Warning ['. $cmd_seq .']: unexpected keyword ('. $val .").\n" }
                     elsif( $verb_flg )
                       { warn 'Skip ['. $cmd_seq .']: '. $val .".\n" }
                    }
                 }
               elsif( $cmd_flg == 1 ) # INSERT or EXPORT command - part one
                 {
                  if( $uc_val eq 'WITH' || $uc_val eq 'SELECT') { $cmd_flg = 2 ; $sql_txt = $val ; @a_sql = () ; }
                  else
                    {
                     if( $uc_val ne 'INTO' && $uc_val ne 'USING' && $uc_val =~ /\w+/ )
                       {
                        if( ! defined $obj_name ) { $obj_name = ($cmd_tp eq 'I') ? $uc_val : $val } # table name OR file name (export)
                        else
                          { $cmd_flg = 0 ; print STDERR 'Error ['. $cmd_seq .']: unexpected identifier ('. $val .').'."\n" ; last ; }
                       }
                    }
                 }
               elsif( $cmd_flg == 2 ) # INSERT or EXPORT command - SELECT command begin collect
                 { $sql_txt .= ' '. $val }
               elsif( $cmd_flg == 3 ) # IMPORT command
                 {
                  if( $uc_val ne 'INTO' && $uc_val =~ /\w+/ )
                    {
                     if(    ! defined $datafile ) { $datafile = $val }
                     elsif( ! defined $obj_name )
                       {
                        $obj_name = $uc_val ;
                        tab_imp( $lda_1, $datafile, $obj_name, '|', $single_flg, $verb_flg ) ;
                        $cmd_flg = 0 ; $datafile = $obj_name = undef ;
                       }
                    }
                 }
              }
            @a_val = () ;
            if( $cmd_flg == 2 ) { push( @a_sql, $sql_txt ) }
           }
        }
      close( $in ) ;
     }
  }

