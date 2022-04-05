import sys
import threading
import time

"""
Progress bar for multiple concurrent progresses.
"""
class ConcurrentProgressBar:
    BARS_WIDTH = 70
    BAR_START = '%s%% ['
    BAR_START_EMPTY = '---% ['
    BAR_END = '] '
    BAR_MIN_POINTS = 5
    
    def __init__(self, num_parts):
        self.num_parts = num_parts
        self.parts = [(0, 0) for _ in range(self.num_parts+1)]
        self.last_length = 0
        self.has_data = False
        max_points = ConcurrentProgressBar.BARS_WIDTH - self.num_parts * len((ConcurrentProgressBar.BAR_START % 100) + ConcurrentProgressBar.BAR_END)
        self.points_per_bar = max(int(max_points / self.num_parts), ConcurrentProgressBar.BAR_MIN_POINTS)
        self.thread = None
        self.run = False
    
    def start(self):
        self.thread = threading.Thread(target=ConcurrentProgressBar._thread_entry, args=(self,))
        self.run = True
        self.thread.start()
    
    def stop(self):
        self.run = False
        self.thread.join()
    
    def update_part(self, i, progress, total):
        self.parts[i] = (progress, total)
        self.has_data = True
    
    def update_total(self, progress, total):
        self.parts[-1] = (progress, total)
        self.has_data = True
    
    def clear(self):
        if self.has_data:
            sys.stdout.write((' ' * self.last_length) + '\r')
            sys.stdout.flush()
    
    def redraw(self):
        if self.has_data:
            line = []
            for i in range(self.num_parts):
                (p, t) = self.parts[i]
                perc_text = self._format_progress_percent(p, t)
                bar_text = self._format_progress_bar(self.points_per_bar, p, t)
                line.append(ConcurrentProgressBar.BAR_START % perc_text)
                line.append(bar_text)
                line.append(ConcurrentProgressBar.BAR_END)
            (p, t) = self.parts[-1]
            line.append('(% 4d/% 4d)' % (p, t))
            line = ''.join(line)
            self.last_length = len(line)
            sys.stdout.write(line)
            sys.stdout.write('\r')
            sys.stdout.flush()
    
    def _format_progress_percent(self, current, total):
        if total == 0:
            return '  0%'
        current = min(max(0, current), total)
        return ('%d%%' % round(current * 100 / total)).rjust(4)
    
    def _format_progress_bar(self, width, current, total):
        if total == 0:
            return '-' * width
        current = min(max(0, current), total)
        num_progress_full = int(current * width / total)
        num_progress_empty = width - num_progress_full
        return ('#' * num_progress_full) + ('-' * num_progress_empty)

    def _thread_entry(self):
        while self.run:
            self.redraw()
            time.sleep(0.5)
